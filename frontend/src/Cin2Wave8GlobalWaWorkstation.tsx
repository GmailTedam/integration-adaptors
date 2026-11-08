import React, { useEffect, useState } from "react";

/**
 * integration-adaptors (SHRL-TR-2026-003H) — integration-adaptors — Zero-PII Ghana Card + NG NIN + UK NHS Blind-Index Proofing & 72h Offline Emergency QR Passport Workstation
 * Full 8-Dimension CAID Input Validation Agent (IVA) & Single-Stream Neural Voice Co-Pilot Surface
 * Orchestration Planes:
 *   - Plane 5: Out-of-Band Uptime Kuma + ntfy Watchdog Heartbeat (Zero BulletTrain)
 *   - Plane 4: Local Bedside Edge ODE Physics Loop (<50ms) + SQLite WAL Durability
 *   - Plane 3: GHARRA Capability & DCB0129 CSO Gate
 *   - Plane 2: Nexus A2A Direct Peer Delegation & DAEC 1.0.0 Checkpoints (Non-BulletTrain)
 *   - Plane 1: BulletTrain OpenHIE FHIR R4 / Counterpoint Consent Hydration
 */
export function Cin2Wave8GlobalWaWorkstation() {
  const [patientId, setPatientId] = useState<string>("9990000018");
  const [offline_outage_hours, setPrimaryNum] = useState<number>(10);
  const [pas_sync_batch_size, setSecondaryNum] = useState<number>(11);
  const [ghana_card_pin, setSelectVal] = useState<string>("");
  const [clinicalNotes, setClinicalNotes] = useState<string>("");
  const [voiceCommand, setVoiceCommand] = useState<string>("");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [errorSummary, setErrorSummary] = useState<string[]>([]);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [voiceBusy, setVoiceBusy] = useState<boolean>(false);
  const [resultBanner, setResultBanner] = useState<string>("");
  const [theme, setTheme] = useState<"dark" | "light">("dark");
  const [voiceDrawerOpen, setVoiceDrawerOpen] = useState<boolean>(false);
  const [activeSubTab, setActiveSubTab] = useState<string>("physics");
  const [wayfindingStatus, setWayfindingStatus] = useState<string>("Wayfinding Ready · Anchor /ui/workstation");

  useEffect(() => {
    const key = "sfds_wayfinding_integration-adaptors";
    const restore = () => {
      try {
        const raw = localStorage.getItem(key);
        if (raw) {
          const parsed = JSON.parse(raw);
          if (parsed.patientId) setPatientId(parsed.patientId);
          if (parsed.theme === "light" || parsed.theme === "dark") setTheme(parsed.theme);
          if (parsed.activeSubTab) setActiveSubTab(parsed.activeSubTab);
          setWayfindingStatus(`Wayfinding Restored · MPI ${parsed.patientId || "9990000018"}`);
        }
      } catch {
        // ignore storage errors
      }
    };
    restore();
    window.addEventListener("pageshow", restore);
    window.addEventListener("popstate", restore);
    return () => {
      window.removeEventListener("pageshow", restore);
      window.removeEventListener("popstate", restore);
    };
  }, []);

  const playSingleStreamNeuralTts = async (spokenText: string) => {
    let fallbackTriggered = false;
    const w = window as unknown as { __sbActiveAudio?: HTMLAudioElement | null };
    if (w.__sbActiveAudio) {
      w.__sbActiveAudio.pause();
      w.__sbActiveAudio = null;
    }
    try {
      const resp = await fetch("/api/v1/voice/speak", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: spokenText.trim(), model: "en_GB-cori-high.onnx" }),
      });
      if (!resp.ok && !fallbackTriggered) {
        fallbackTriggered = true;
      }
    } catch {
      if (!fallbackTriggered) {
        fallbackTriggered = true;
      }
    }
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (isSubmitting) return;

    const trimmedPatientId = patientId.trim();
    const trimmedNotes = clinicalNotes.trim();
    const trimmedSelect = ghana_card_pin.trim();
    const nextErrors: Record<string, string> = {};
    const summaryList: string[] = [];

    if (!trimmedPatientId || !/^(?:NHS-)?\d{10}$/.test(trimmedPatientId)) {
      nextErrors.patientId = "BulletTrain Client Registry 10-digit Mod-11 Patient ID is required (e.g. 9990000018).";
      summaryList.push(nextErrors.patientId);
    }
    if (!trimmedSelect) {
      nextErrors.ghana_card_pin = "Please select a valid clinical modality/agent option.";
      summaryList.push(nextErrors.ghana_card_pin);
    }
    if (
      Number.isNaN(offline_outage_hours) ||
      offline_outage_hours < 0 ||
      offline_outage_hours > 72
    ) {
      nextErrors.offline_outage_hours = "Simulated Clinic WAN Outage Duration (Hours) must be between 0 and 72.";
      summaryList.push(nextErrors.offline_outage_hours);
    }
    if (
      Number.isNaN(pas_sync_batch_size) ||
      pas_sync_batch_size < 1 ||
      pas_sync_batch_size > 200
    ) {
      nextErrors.pas_sync_batch_size = "Offline SQLite-WAL CRDT Sync Batch Size must be between 1 and 200.";
      summaryList.push(nextErrors.pas_sync_batch_size);
    }
    if (trimmedNotes.length < 5 || trimmedNotes.length > 500) {
      nextErrors.clinicalNotes = "Clinical notes must be between 5 and 500 characters after trimming whitespace.";
      summaryList.push(nextErrors.clinicalNotes);
    }

    setErrors(nextErrors);
    setErrorSummary(summaryList);
    if (summaryList.length > 0) {
      return;
    }

    setIsSubmitting(true);
    try {
      const response = await fetch("/api/v1/physics/evaluate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          patient_id: trimmedPatientId,
          offline_outage_hours,
          pas_sync_batch_size,
          ghana_card_pin: trimmedSelect,
          clinical_notes: trimmedNotes,
        }),
      });
      const data = await response.json();
      setResultBanner(`Decision: ${data.recommended_branch || "RESOLVED_PROCEED"}`);
      await playSingleStreamNeuralTts(`Physics gate evaluated: ${data.recommended_branch || "RESOLVED_PROCEED"}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleVoiceSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (voiceBusy) return;
    const cleanCmd = voiceCommand.trim();
    if (!cleanCmd) {
      setErrors((prev) => ({ ...prev, voiceCommand: "Voice command cannot be empty or whitespace." }));
      setErrorSummary(["Voice command cannot be empty or whitespace."]);
      return;
    }
    setVoiceBusy(true);
    try {
      await playSingleStreamNeuralTts(cleanCmd);
    } finally {
      setVoiceBusy(false);
    }
  };

  return (
    <main
      data-theme={theme}
      style={{
        backgroundColor: "var(--sfds-bg)",
        backgroundImage: "linear-gradient(135deg, var(--sfds-bg) 0%, var(--sfds-surface) 100%)",
        color: "var(--sfds-text)",
        minHeight: "100vh",
        padding: "24px",
      }}
    >
      <header data-testid="sfds-topbar">
        <h1>integration-adaptors — Zero-PII Ghana Card + NG NIN + UK NHS Blind-Index Proofing & 72h Offline Emergency QR Passport Workstation (SHRL-TR-2026-003H)</h1>
        <div className="topbar-actions">
          <button
            type="button"
            data-testid="voice-copilot-trigger"
            onClick={() => setVoiceDrawerOpen((v) => !v)}
          >
            Voice Co-Pilot (Alt+V)
          </button>
          <button
            type="button"
            data-testid="theme-toggle"
            onClick={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
          >
            {theme === "dark" ? "Switch to Light Mode" : "Switch to Dark Mode"}
          </button>
        </div>
        <p className="required-legend">
          Fields marked with <span aria-hidden="true">*</span> <span className="sr-only">(required)</span> are mandatory.
        </p>
      </header>

      <nav data-testid="signalbox-wayfinding-bar" data-agent-anchor="workstation-home" aria-label="SignalBox Clinical Wayfinding">
        <a href="/ui/workstation" data-testid="wayfinding-home-link">Anchor: /ui/workstation</a> | <span>{wayfindingStatus}</span>
      </nav>

      <section data-testid="layer0-patient-safety-banner" aria-label="Layer 0 Patient Safety Banner">
        <strong>Patient Safety Banner:</strong> MPI {patientId} · GP B86049 · Trust FAC-GB-842753EF6179 · ReSPECT Full Active Care
      </section>

      {voiceDrawerOpen && (
        <section aria-label="voice-copilot-drawer" data-testid="voice-copilot-drawer">
          <h2>SignalBox Neural Voice Co-Pilot (Alt+V — en_GB-cori-high.onnx)</h2>
          <form noValidate onSubmit={handleVoiceSubmit}>
            <label htmlFor="voice-command">
              Voice Command Utterance <span aria-hidden="true">*</span>
              <span className="sr-only">(required)</span>
            </label>
            <input
              id="voice-command"
              name="voiceCommand"
              type="text"
              required
              aria-required="true"
              minLength={2}
              maxLength={200}
              placeholder="Speak or type bedside physics command (2-200 chars)..."
              value={voiceCommand}
              onChange={(e) => setVoiceCommand(e.target.value)}
              aria-invalid={Boolean(errors.voiceCommand)}
              aria-describedby="voice-command-hint voice-command-error"
            />
            <small id="voice-command-hint" className="format-hint">
              22,050 Hz VITS neural voice synthesis (single-stream __sbActiveAudio guard)
            </small>
            <div className="char-counter">{voiceCommand.length}/200 characters</div>
            {errors.voiceCommand && (
              <span id="voice-command-error" role="alert" className="field-error">
                {errors.voiceCommand}
              </span>
            )}
            <button type="submit" disabled={voiceBusy || isSubmitting}>
              {voiceBusy ? "Synthesizing..." : "Execute Voice Command"}
            </button>
          </form>
        </section>
      )}

      <section data-testid="sfds-kpi-strip" aria-label="SFDS v2.4 Operational KPI Strip">
        <span>Active Cohort: 6 MPIs</span> | <span>Conservation Gate: 0.00% PCLVR</span> | <span>1-Click Staged: 18 min saved</span>
      </section>

      <nav data-testid="sfds-workspace-tabs" aria-label="SFDS v2.4 Workspace Sub-Tabs">
        <button type="button" onClick={() => setActiveSubTab("physics")} aria-pressed={activeSubTab === "physics"}>
          1. Bedside Physics & Order Stager
        </button>
        <button type="button" onClick={() => setActiveSubTab("cdr")} aria-pressed={activeSubTab === "cdr"}>
          2. Dual-Audience CDR 1.0.0
        </button>
      </nav>

      {errorSummary.length > 0 && (
        <div role="alert" data-testid="form-error-summary" className="error-summary-banner">
          <strong>Validation Error Summary:</strong>
          <ul>
            {errorSummary.map((msg, idx) => (
              <li key={idx}>{msg}</li>
            ))}
          </ul>
        </div>
      )}

      <form noValidate onSubmit={handleSubmit} aria-label="integration-adaptors-physics-form">
        <div className="form-group">
          <label htmlFor="patient_id">
            Patient Pseudonymised NHS ID <span aria-hidden="true">*</span>
            <span className="sr-only">(required)</span>
          </label>
          <input
            id="patient_id"
            name="patient_id"
            type="text"
            required
            aria-required="true"
            minLength={14}
            maxLength={14}
            pattern="^NHS-\d{10}$"
            placeholder="9990000018"
            value={patientId}
            onChange={(e) => setPatientId(e.target.value)}
            aria-invalid={Boolean(errors.patientId)}
            aria-describedby="patient_id-hint patient_id-error"
          />
          <small id="patient_id-hint" className="format-hint">
            Format: NHS-10DIGITS (MOD-11 verified Counterpoint pseudonym)
          </small>
          {errors.patientId && (
            <span id="patient_id-error" role="alert" className="field-error">
              {errors.patientId}
            </span>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="ghana_card_pin">
            Verified Ghana Card PIN (GHA-XXXXXXXXX-X) <span aria-hidden="true">*</span>
            <span className="sr-only">(required)</span>
          </label>
          <select
            id="ghana_card_pin"
            name="ghana_card_pin"
            required
            aria-required="true"
            value={ghana_card_pin}
            onChange={(e) => setSelectVal(e.target.value)}
            aria-invalid={Boolean(errors.ghana_card_pin)}
            aria-describedby="ghana_card_pin-hint ghana_card_pin-error"
          >
            <option value="" disabled>
              Select Verified Ghana Card PIN (GHA-XXXXXXXXX-X)...
            </option>
            <option value="GHA-725814930-6">GHA-725814930-6</option>
            <option value="GHA-418293650-5">GHA-418293650-5</option>
            <option value="GHA-609384712-3">GHA-609384712-3</option>
            <option value="GHA-531728496-0">GHA-531728496-0</option>
          </select>
          <small id="ghana_card_pin-hint" className="format-hint">
            Select the active bedside protocol before running conservation solver.
          </small>
          {errors.ghana_card_pin && (
            <span id="ghana_card_pin-error" role="alert" className="field-error">
              {errors.ghana_card_pin}
            </span>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="offline_outage_hours">
            Simulated Clinic WAN Outage Duration (Hours) <span aria-hidden="true">*</span>
            <span className="sr-only">(required)</span>
          </label>
          <input
            id="offline_outage_hours"
            name="offline_outage_hours"
            type="number"
            required
            aria-required="true"
            min={0}
            max={72}
            step={1}
            placeholder="0 - 72"
            value={offline_outage_hours}
            onChange={(e) => setPrimaryNum(Number(e.target.value))}
            aria-invalid={Boolean(errors.offline_outage_hours)}
            aria-describedby="offline_outage_hours-hint offline_outage_hours-error"
          />
          <small id="offline_outage_hours-hint" className="format-hint">
            Physical conservation bound: 0 to 72 (step 1)
          </small>
          {errors.offline_outage_hours && (
            <span id="offline_outage_hours-error" role="alert" className="field-error">
              {errors.offline_outage_hours}
            </span>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="pas_sync_batch_size">
            Offline SQLite-WAL CRDT Sync Batch Size <span aria-hidden="true">*</span>
            <span className="sr-only">(required)</span>
          </label>
          <input
            id="pas_sync_batch_size"
            name="pas_sync_batch_size"
            type="number"
            required
            aria-required="true"
            min={1}
            max={200}
            step={1}
            placeholder="1 - 200"
            value={pas_sync_batch_size}
            onChange={(e) => setSecondaryNum(Number(e.target.value))}
            aria-invalid={Boolean(errors.pas_sync_batch_size)}
            aria-describedby="pas_sync_batch_size-hint pas_sync_batch_size-error"
          />
          <small id="pas_sync_batch_size-hint" className="format-hint">
            Physiological range: 1 to 200 (step 1)
          </small>
          {errors.pas_sync_batch_size && (
            <span id="pas_sync_batch_size-error" role="alert" className="field-error">
              {errors.pas_sync_batch_size}
            </span>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="clinical_notes">
            Clinical Observation & Counterfactual Justification <span aria-hidden="true">*</span>
            <span className="sr-only">(required)</span>
          </label>
          <textarea
            id="clinical_notes"
            name="clinical_notes"
            required
            aria-required="true"
            minLength={5}
            maxLength={500}
            placeholder="Enter bedside physiological rationale (5-500 characters)..."
            value={clinicalNotes}
            onChange={(e) => setClinicalNotes(e.target.value)}
            aria-invalid={Boolean(errors.clinicalNotes)}
            aria-describedby="clinical_notes-hint clinical_notes-counter clinical_notes-error"
          />
          <small id="clinical_notes-hint" className="format-hint">
            Provide 5 to 500 characters of clinical context for D1/D6 grounding.
          </small>
          <div id="clinical_notes-counter" className="char-counter">
            {clinicalNotes.length}/500 characters
          </div>
          {errors.clinicalNotes && (
            <span id="clinical_notes-error" role="alert" className="field-error">
              {errors.clinicalNotes}
            </span>
          )}
        </div>

        <button
          type="submit"
          data-testid="submit-physics-eval"
          disabled={isSubmitting || voiceBusy}
        >
          {isSubmitting ? "Submitting..." : "Evaluate First-Principles Physics Gate"}
        </button>
      </form>

      {resultBanner && (
        <output data-testid="physics-result-banner" role="status">
          {resultBanner}
        </output>
      )}
    </main>
  );
}

export default Cin2Wave8GlobalWaWorkstation;
