const cds = require("@sap/cds")
const LOG = cds.log("incidents-app")
const OUTBOX_LOG = cds.log("outbox-poll")

cds.once("served", () => {
  process.stderr.write(`[ENV] profiles=${JSON.stringify(cds.env.profiles)}\n`)
  process.stderr.write(`[ENV] bypass_draft=${cds.env.fiori?.bypass_draft}\n`)
  process.stderr.write(
    `[ENV] db=${JSON.stringify(cds.env.requires?.db?.kind)}\n`,
  )
})

const seenIDs = new Set()

const ts = () => new Date().toISOString()

const logOutbox = (label, messages) => {
  messages.forEach((m) => seenIDs.add(m.ID))
  const body = `*** ${label} *** count=${messages.length} cumulative=${seenIDs.size}\n${JSON.stringify(
    messages.map((m) => ({
      ID: m.ID,
      target: m.target,
      status: m.status,
      attempts: m.attempts,
      lastError: m.lastError,
      timestamp: m.timestamp,
      event: (() => {
        try {
          return JSON.parse(m.msg)?.event
        } catch {
          return m.msg
        }
      })(),
    })),
    null,
    2,
  )}`
  const now = ts()
  process.stderr.write(
    body
      .split("\n")
      .map((l) => `${now} ${l}`)
      .join("\n") + "\n",
  )
}

cds.once("served", () => {
  const _originalFlush = cds.flush.bind(cds)
  cds.flush = async function loggedFlush(target, opts) {
    let messages = []
    try {
      messages = await cds.tx(() => SELECT.from("cds.outbox.Messages"))
    } catch {
      /* table not yet deployed */
    }
    logOutbox(`OUTBOX POLL (target=${target ?? "*"})`, messages)
    return _originalFlush(target, opts)
  }

  const timer = setInterval(async () => {
    let messages = []
    try {
      messages = await cds.tx(() => SELECT.from("cds.outbox.Messages"))
    } catch {
      /* table not yet deployed */
    }
    logOutbox(`OUTBOX STATUS (periodic)`, messages)
  }, 5000).unref()
  cds.on("shutdown", () => clearInterval(timer))
})

class ProcessorService extends cds.ApplicationService {
  /** Registering custom event handlers */
  init() {
    const res = super.init()
    this.prepend(() =>
      this.on(
        "PUT",
        this.entities["SampleRootWithComposedEntity.attachments"].drafts,
        async (req, next) => {
          cds
            .log("overwrite-put-handler")
            .info("Overwritten PUT handler called")
          return next()
        },
      ),
    )

    this.before("UPDATE", "Incidents", (req) => this.onUpdate(req))
    this.before(["CREATE", "UPDATE"], "Incidents", (req) =>
      this.changeUrgencyDueToSubject(req.data),
    )

    this.on("insertTestData", () => this.insertTestData())
    this.prepend(() =>
      this.on("*", (req, next) => {
        if (req.path && req.path.endsWith("insertTestData"))
          return this.insertTestData()
        return next()
      }),
    )
    this.on("copyIncident", (req) => this.onCopyIncident(req))

    return res
  }

  async onCopyIncident(req) {
    const { Incidents } = this.entities
    const Attachments = this.entities["Incidents.attachments"]
    const sourceID = req.params[0]?.ID ?? req.params[0]

    // Read source incident fields
    const source = await SELECT.one
      .from(Incidents, { ID: sourceID })
      .columns("title", "customer_ID", "urgency_code")
    if (!source) return req.reject(404, "Source incident not found")

    // Create a new draft incident
    const newDraft = await this.new(Incidents.drafts, {
      title: source.title + " (Copy)",
      customer_ID: source.customer_ID,
      urgency_code: source.urgency_code,
    })

    // Look up the DraftUUID for the new draft
    const draftAdmin = await SELECT.one
      .from(Incidents.drafts, { ID: newDraft.ID })
      .columns("DraftAdministrativeData_DraftUUID")
    if (!draftAdmin?.DraftAdministrativeData_DraftUUID)
      return req.reject(500, "Failed to create draft")

    // Copy all attachments from the active source into the new draft
    const sourceAttachmentsEntity = await SELECT.from(Attachments).where({
      up__ID: sourceID,
    })
    if (sourceAttachmentsEntity.length > 0) {
      const AttachmentsSrv = await cds.connect.to("attachments")
      for (const att of sourceAttachmentsEntity) {
        await AttachmentsSrv.copy(
          Attachments,
          { ID: att.ID },
          Attachments.drafts,
          {
            up__ID: newDraft.ID,
            DraftAdministrativeData_DraftUUID:
              draftAdmin.DraftAdministrativeData_DraftUUID,
          },
        )
      }
    }

    return newDraft
  }

  async insertTestData() {
    const firstID = cds.utils.uuid()
    const secondID = cds.utils.uuid()
    await INSERT.into("sap.capire.incidents.NonDraftTest").entries(
      {
        ID: firstID,
        title: "Test Incident 1",
        description: "This is a test incident 1",
        urgency_code: "L",
        urgency_descr: "Low",
      },
      {
        ID: secondID,
        title: "Urgent Test Incident 2",
        description: "This is a test incident 2",
        urgency_code: "L",
        urgency_descr: "Low",
      },
    )
    await INSERT.into("sap.capire.incidents.NonDraftTest.attachments").entries(
      {
        ID: cds.utils.uuid(),
        up__ID: firstID,
        fileName: "test1.txt",
        mimeType: "text/plain",
        content: Buffer.from("Hello World 1"),
      },
      {
        ID: cds.utils.uuid(),
        up__ID: secondID,
        fileName: "test2.txt",
        mimeType: "text/plain",
        content: Buffer.from("Hello World 2"),
      },
    )
    LOG.info("Test data inserted into NonDraftTest and attachments")
    return "Test data inserted"
  }

  changeUrgencyDueToSubject(data) {
    if (data) {
      const incidents = Array.isArray(data) ? data : [data]
      incidents.forEach((incident) => {
        if (incident.title?.toLowerCase().includes("urgent")) {
          incident.urgency = { code: "H", descr: "High" }
        }
      })
    }
  }

  /** Custom Validation */
  async onUpdate(req) {
    const { status_code } = await SELECT.one(
      req.subject,
      (i) => i.status_code,
    ).where({ ID: req.data.ID })
    if (status_code === "C") {
      return req.reject(`Can't modify a closed incident`)
    }
  }
}

module.exports = { ProcessorService }
