import { useState, useEffect, useCallback } from "react";

// ─── Types ──────────────────────────────────────────────────────────────────

interface QuoteSteps {
  received: boolean;
  parsed: boolean;
  enriched: boolean;
  features: boolean;
  risk_scoring: boolean;
  quote_review: boolean;
  quote_creation: boolean;
  pdf_created: boolean;
  completed: boolean;
  response_email: boolean;
}

interface Quote {
  email_id: string;
  file_name: string;
  ingestion_timestamp: string;
  business_name: string;
  risk_category: string;
  sender_name: string;
  sender_email: string;
  annual_revenue: number | null;
  num_employees: number | null;
  coverages_requested: string;
  decision_tag: string | null;
  uw_notes: string | null;
  uw_surcharge_pct: number | null;
  uw_discount_pct: number | null;
  uw_decided_at: string | null;
  uw_info_request: string | null;
  risk_score: number | null;
  risk_band: string | null;
  review_summary: string | null;
  total_premium: number | null;
  adjusted_premium: number | null;
  quote_number: string | null;
  pdf_path: string | null;
  pdf_status: string | null;
  final_status: string | null;
  steps: QuoteSteps;
}

type Page = "email_intake" | "processing" | "placeholder1" | "analytics" | "quotes";

interface SampleEmail {
  org_id: string;
  label: string;
  business_name: string;
  risk_category: string;
  risk_level: "low" | "medium" | "high";
  sender_name: string;
  sender_email: string;
  num_employees: number;
  annual_revenue: number;
  body_preview: string;
}

// ─── Constants ──────────────────────────────────────────────────────────────

const STEP_KEYS: (keyof QuoteSteps)[] = [
  "received", "parsed", "enriched", "features", "risk_scoring",
  "quote_review", "quote_creation", "pdf_created", "completed", "response_email",
];

const STEP_LABELS: string[] = [
  "Email Received", "LLM Parsing", "Data Enrichment", "Featurization",
  "Risk Scoring", "Quote Review", "Quote Creation", "PDF Creation", "Completed", "Response Sent",
];

// ─── Helpers ────────────────────────────────────────────────────────────────

function relativeTime(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  if (diffMs < 0) return "just now";
  const s = Math.floor(diffMs / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

function pdfUrl(pdfPath: string | null | undefined): string | null {
  if (!pdfPath) return null;
  // Extract quote number from volume path like /Volumes/.../QT-20260325-abc12345.pdf
  const match = pdfPath.match(/(QT-[^/]+)\.pdf$/);
  return match ? `/api/pdf/${match[1]}` : null;
}

function formatCurrency(value: number | null): string {
  if (value == null) return "--";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
}

// ─── Badges ─────────────────────────────────────────────────────────────────

function RiskBadge({ band }: { band: string | null }) {
  if (!band) return null;
  const c: Record<string, string> = {
    Low: "bg-emerald-500/10 text-emerald-400", Medium: "bg-amber-500/10 text-amber-400",
    High: "bg-orange-500/10 text-orange-400", "Very High": "bg-red-500/10 text-red-400",
  };
  return <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${c[band] || "bg-white/10 text-white/50"}`}>{band}</span>;
}

function DecisionBadge({ tag }: { tag: string | null }) {
  if (!tag) return null;
  const c: Record<string, string> = {
    "auto-approved": "bg-emerald-500/10 text-emerald-400",
    "pending-review": "bg-amber-500/10 text-amber-400",
    "auto-declined": "bg-red-500/10 text-red-400",
    "uw-approved": "bg-emerald-500/10 text-emerald-400",
    "uw-declined": "bg-red-500/10 text-red-400",
    "uw-info": "bg-blue-500/10 text-blue-400",
  };
  return <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${c[tag] || "bg-white/10 text-white/50"}`}>{tag}</span>;
}

// ─── Mini Pipeline (row summary) ────────────────────────────────────────────

function MiniPipeline({ steps, decisionTag }: { steps: QuoteSteps; decisionTag: string | null }) {
  const flags = STEP_KEYS.map((k) => steps[k]);
  const first = flags.indexOf(false);
  const pending = decisionTag === "pending-review";
  return (
    <div className="flex items-center gap-0.5">
      {flags.map((done, i) => {
        const warn = pending && i === 6;
        const spin = !steps.completed && !warn && i === first;
        return (
          <div key={i} className="flex items-center">
            {i > 0 && <div className={`w-1.5 h-px ${done ? "bg-emerald-400" : warn ? "bg-amber-400" : "bg-white/20"}`} />}
            <div className={`w-2.5 h-2.5 rounded-full ${
              done ? "bg-emerald-500" : warn ? "bg-amber-400" : spin ? "border border-brick-primary border-t-transparent step-spinner" : "bg-white/20"
            }`} />
          </div>
        );
      })}
    </div>
  );
}

// ─── Step Detail Panel ──────────────────────────────────────────────────────

const COL_LABELS: Record<string, string> = {
  email_id: "Email ID", file_name: "File Name", file_size: "File Size (bytes)",
  file_modification_time: "File Modified", ingestion_timestamp: "Ingested At",
  parse_timestamp: "Parsed At", sender_name: "Sender", sender_email: "Email",
  sender_phone: "Phone", sender_title: "Title", business_name: "Business",
  business_dba: "DBA", naics_code: "NAICS", industry_description: "Industry",
  risk_category: "Risk Category", date_established: "Established", num_locations: "Locations",
  location_states: "States", annual_revenue: "Revenue", annual_payroll: "Payroll",
  num_employees: "Employees", coverages_requested: "Coverages Requested",
  gl_limit_requested: "GL Limit", property_tiv: "Property TIV", auto_fleet_size: "Fleet Size",
  cyber_limit_requested: "Cyber Limit", umbrella_limit_requested: "Umbrella Limit",
  num_claims_5yr: "Claims (5yr)", total_claims_amount: "Claims Amount",
  worst_claim_description: "Worst Claim", claim_types: "Claim Types",
  current_carrier: "Current Carrier", current_premium: "Current Premium",
  renewal_date: "Renewal Date", has_safety_procedures: "Safety Procedures",
  has_employee_training: "Employee Training", special_requirements: "Special Requirements",
  urgency: "Urgency", enrichment_timestamp: "Enriched At",
  is_existing_customer: "Existing Customer", industry_avg_claims_per_org: "Ind. Avg Claims/Org",
  industry_avg_claim_severity: "Ind. Avg Claim Severity", industry_avg_premium: "Ind. Avg Premium",
  industry_avg_revenue: "Ind. Avg Revenue", industry_avg_employees: "Ind. Avg Employees",
  revenue_vs_industry_pct: "Revenue vs Ind. %", premium_vs_industry_pct: "Premium vs Ind. %",
  claims_vs_industry: "Claims vs Industry", heuristic_risk_score: "Heuristic Score",
  feature_timestamp: "Features At", risk_score: "Risk Score", risk_band: "Risk Band",
  claim_prediction: "Claim Prediction", predicted_loss_ratio: "Loss Ratio",
  pricing_action: "Pricing Action", underwriting_action: "UW Action",
  scoring_method: "Scoring Method", scoring_timestamp: "Scored At",
  decision_tag: "Decision", review_summary: "Review Summary", review_timestamp: "Reviewed At",
  quote_number: "Quote #", total_premium: "Total Premium", subtotal_premium: "Subtotal",
  gl_premium: "GL Premium", liquor_premium: "Liquor", property_building_premium: "Property Bldg",
  property_contents_premium: "Property Contents", bi_premium: "Business Income",
  equipment_premium: "Equipment", wc_premium: "Workers Comp", auto_premium: "Auto",
  cyber_premium: "Cyber", umbrella_premium: "Umbrella", terrorism_premium: "Terrorism (TRIA)",
  policy_fees: "Fees", surcharge_pct: "Surcharge %", discount_pct: "Discount %",
  risk_mult: "Risk Mult.", industry_mult: "Industry Mult.", experience_mod: "Exp. Mod",
  effective_date: "Effective", expiration_date: "Expiration", underwriter_notes: "UW Notes",
  quote_timestamp: "Quote At", pdf_path: "PDF Path", pdf_status: "PDF Status",
  pdf_executive_summary: "Executive Summary", pdf_error: "PDF Error",
  pdf_timestamp: "PDF At", final_status: "Final Status", completed_timestamp: "Completed At",
  email_subject: "Email Subject", email_body: "Email Body", eml_file_name: "Email File",
  eml_volume_path: "Volume Path", eml_write_status: "Write Status", response_timestamp: "Response At",
};

function fmtVal(key: string, value: unknown): string {
  if (value == null) return "--";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") {
    if (key.includes("premium") || key.includes("revenue") || key.includes("payroll")
        || key.includes("amount") || key === "current_premium" || key.includes("_limit")
        || key === "total_premium" || key === "subtotal_premium")
      return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
    if (key.includes("pct") || key.includes("ratio") || key.includes("mult") || key.includes("mod")) return value.toFixed(2);
    if (key === "risk_score" || key === "heuristic_risk_score") return value.toFixed(1);
    return value.toLocaleString();
  }
  if (typeof value === "string" && value.match(/^\d{4}-\d{2}-\d{2}T/)) return new Date(value).toLocaleString();
  return String(value);
}

function PdfModal({ pdfPath, onClose }: { pdfPath: string; onClose: () => void }) {
  return (
    <div
      className="fixed z-[60] bg-black/50 backdrop-blur-sm"
      style={{ top: 0, left: 0, right: 0, bottom: 0, display: "grid", placeItems: "center" }}
      onClick={onClose}
    >
      <div
        className="bg-[#1e2030] rounded-xl shadow-2xl flex flex-col overflow-hidden"
        style={{ width: "min(92vw, 1100px)", height: "85vh" }}
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-3 border-b border-white/5 bg-sidebar shrink-0">
          <div className="flex items-center gap-2">
            <svg className="w-4 h-4 text-brick-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
            </svg>
            <span className="text-sm font-medium text-white">Quote Document</span>
          </div>
          <button onClick={onClose} className="text-white/40 hover:text-white transition-colors">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <iframe src={pdfPath} className="flex-1 w-full" title="PDF Quote" />
        <div className="flex justify-end px-5 py-3 border-t border-white/5 bg-[#13141f] shrink-0">
          <button
            onClick={onClose}
            className="px-5 py-2 bg-brick-primary hover:bg-brick-primary/90 text-white text-sm font-medium rounded-lg transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

function StepDetailPanel({ data, stepLabel, pdfPath }: {
  data: Record<string, unknown>; stepLabel: string; pdfPath?: string | null;
}) {
  const [showPdf, setShowPdf] = useState(false);
  const entries = Object.entries(data).filter(([k]) => k !== "email_id");
  const timestamps = entries.filter(([k]) => k.includes("timestamp") || k.endsWith("_at"));
  const main = entries.filter(([k]) => !k.includes("timestamp") && !k.endsWith("_at"));

  return (
    <div className="expand-enter bg-[#1e2030] border border-white/10 rounded-lg p-5 mt-4">
      <div className="flex items-center justify-between mb-4">
        <h4 className="text-xs font-semibold text-white/40 uppercase tracking-wider">{stepLabel}</h4>
        {pdfPath && (
          <button
            onClick={() => setShowPdf(true)}
            className="inline-flex items-center gap-2 px-4 py-2 bg-brick-primary hover:bg-brick-primary/90 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
            </svg>
            View PDF Quote
          </button>
        )}
      </div>
      {showPdf && pdfPath && <PdfModal pdfPath={pdfUrl(pdfPath) || pdfPath} onClose={() => setShowPdf(false)} />}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-8 gap-y-1">
        {main.map(([key, val]) => {
          const label = COL_LABELS[key] || key.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
          const long = typeof val === "string" && String(val).length > 80;
          return (
            <div key={key} className={`flex flex-col py-2 border-b border-white/5 ${long ? "sm:col-span-2 lg:col-span-3" : ""}`}>
              <span className="text-[10px] text-white/40 uppercase tracking-wider">{label}</span>
              <span className={`text-[13px] text-white/70 ${long ? "leading-relaxed mt-0.5" : "font-medium"}`}>{fmtVal(key, val)}</span>
            </div>
          );
        })}
      </div>
      {timestamps.length > 0 && (
        <div className="mt-4 pt-3 border-t border-white/5 flex flex-wrap gap-6">
          {timestamps.map(([key, val]) => (
            <div key={key} className="text-[11px] text-white/40">
              <span className="uppercase tracking-wide">{COL_LABELS[key] || key}: </span>
              <span className="text-white/50">{fmtVal(key, val)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Interactive Pipeline (clickable steps) ─────────────────────────────────

function InteractivePipeline({ steps, decisionTag, emailId, pdfPath }: {
  steps: QuoteSteps; decisionTag: string | null; emailId: string; pdfPath?: string | null;
}) {
  const flags = STEP_KEYS.map((k) => steps[k]);
  const first = flags.indexOf(false);
  const pending = decisionTag === "pending-review";

  const [sel, setSel] = useState<number | null>(null);
  const [data, setData] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);

  const click = async (i: number) => {
    if (!flags[i]) return;
    if (sel === i) { setSel(null); setData(null); return; }
    setSel(i); setData(null); setLoading(true);
    try {
      const r = await fetch(`/api/quotes/${emailId}/step/${STEP_KEYS[i]}`);
      if (r.ok) setData(await r.json());
    } catch { /* silent */ } finally { setLoading(false); }
  };

  return (
    <div>
      <div className="flex items-center justify-between w-full py-5 px-1">
        {STEP_KEYS.map((_, i) => {
          const done = flags[i];
          const warn = pending && i === 6;
          const spin = !steps.completed && !warn && i === first;
          const active = sel === i;
          return (
            <div key={i} className="flex items-center flex-1 last:flex-none">
              <div className="flex flex-col items-center min-w-[72px]">
                <button
                  onClick={() => click(i)}
                  disabled={!done}
                  className={`w-9 h-9 rounded-full flex items-center justify-center transition-all duration-150 ${
                    done ? "cursor-pointer hover:scale-110 active:scale-95" : "cursor-default"
                  } ${
                    active ? "bg-brick-dark ring-2 ring-brick-primary ring-offset-2 ring-offset-[#1e2030]"
                    : done ? "bg-emerald-500"
                    : warn ? "bg-amber-400"
                    : spin ? "border-[3px] border-brick-primary border-t-transparent step-spinner bg-[#1e2030]"
                    : "bg-white/10"
                  }`}
                >
                  {(done || active) && (
                    <svg className="w-4 h-4 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                  )}
                  {warn && !done && (
                    <svg className="w-4 h-4 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                    </svg>
                  )}
                  {!done && !spin && !warn && <span className="text-[11px] text-white/40 font-medium">{i + 1}</span>}
                </button>
                <span className={`text-[10px] mt-1.5 text-center leading-tight font-medium ${
                  active ? "text-brick-primary" : done ? "text-emerald-400" : warn ? "text-amber-400" : spin ? "text-brick-primary" : "text-white/40"
                }`}>
                  {warn ? "Pending Review" : STEP_LABELS[i]}
                </span>
              </div>
              {i < STEP_KEYS.length - 1 && (
                <div className={`flex-1 h-px mx-0.5 -mt-5 ${flags[i + 1] ? "bg-emerald-400" : done ? "bg-emerald-400/30" : warn ? "bg-amber-400/30" : "bg-white/10"}`} />
              )}
            </div>
          );
        })}
      </div>

      {sel !== null && (
        <div className="pb-1">
          {loading ? (
            <div className="flex items-center gap-2 py-8 justify-center text-white/40">
              <div className="w-4 h-4 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
              <span className="text-sm">Loading...</span>
            </div>
          ) : data ? (
            <StepDetailPanel
              data={data}
              stepLabel={STEP_LABELS[sel]}
              pdfPath={STEP_KEYS[sel] === "completed" || STEP_KEYS[sel] === "pdf_created" ? pdfPath : null}
            />
          ) : (
            <div className="py-6 text-center text-sm text-white/40">No data available</div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Quote Row + Expanded Details ───────────────────────────────────────────

function QuoteRow({ quote, isExpanded, onToggle }: {
  quote: Quote; isExpanded: boolean; onToggle: () => void;
}) {
  return (
    <div className={`bg-[#1e2030] rounded-lg border transition-all duration-150 ${
      isExpanded ? "border-white/20 shadow-sm" : "border-white/5 hover:border-white/10"
    }`}>
      <div className="px-5 py-3.5 cursor-pointer select-none" onClick={onToggle}>
        <div className="flex items-center gap-5">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <h3 className={`text-sm font-semibold truncate ${quote.business_name ? "text-white" : "text-white/40 italic"}`}>{quote.business_name || "Processing new request\u2026"}</h3>
              <RiskBadge band={quote.risk_band} />
              <DecisionBadge tag={quote.decision_tag} />
              {quote.total_premium != null && (
                <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${quote.adjusted_premium != null ? "bg-white/5 text-white/40 line-through" : "bg-white/10 text-white/70"}`}>{formatCurrency(quote.total_premium)}</span>
              )}
              {quote.adjusted_premium != null && (
                <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-emerald-500/10 text-emerald-400">{formatCurrency(quote.adjusted_premium)}</span>
              )}
            </div>
            <p className="text-[12px] text-white/40 mt-0.5 truncate">{quote.business_name ? <>{quote.sender_name} &middot; {quote.sender_email}</> : quote.file_name}</p>
          </div>
          <div className="hidden md:block"><MiniPipeline steps={quote.steps} decisionTag={quote.decision_tag} /></div>
          <p className="text-[11px] text-white/40 min-w-[60px] text-right">{relativeTime(quote.ingestion_timestamp)}</p>
          <svg className={`w-4 h-4 text-white/40 transition-transform duration-150 shrink-0 ${isExpanded ? "rotate-180" : ""}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </div>
      {isExpanded && (
        <div className="expand-enter border-t border-white/5 px-5 pt-3 pb-4">
          {/* Underwriter notes */}
          {(quote.uw_notes || quote.uw_info_request) && (
            <div className="mb-4 p-4 rounded-lg border border-white/10 bg-[#13141f]">
              <h4 className="text-xs font-semibold text-white/50 uppercase tracking-wider mb-2 flex items-center gap-2">
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25z" />
                </svg>
                Underwriter Decision
              </h4>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {quote.uw_notes && (
                  <div className="sm:col-span-2">
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Notes</p>
                    <p className="text-sm text-white/70 mt-0.5 leading-relaxed">{quote.uw_notes}</p>
                  </div>
                )}
                {quote.uw_info_request && (
                  <div className="sm:col-span-2">
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Information Requested</p>
                    <p className="text-sm text-white/70 mt-0.5 leading-relaxed">{quote.uw_info_request}</p>
                  </div>
                )}
                {quote.uw_surcharge_pct != null && quote.uw_surcharge_pct > 0 && (
                  <div>
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Surcharge</p>
                    <p className="text-sm font-medium text-white/80 mt-0.5">{quote.uw_surcharge_pct}%</p>
                  </div>
                )}
                {quote.uw_discount_pct != null && quote.uw_discount_pct > 0 && (
                  <div>
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Discount</p>
                    <p className="text-sm font-medium text-white/80 mt-0.5">{quote.uw_discount_pct}%</p>
                  </div>
                )}
                {quote.uw_decided_at && (
                  <div>
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Decided At</p>
                    <p className="text-sm text-white/60 mt-0.5">{new Date(quote.uw_decided_at).toLocaleString()}</p>
                  </div>
                )}
              </div>
            </div>
          )}
          <p className="text-[11px] text-white/40 mb-1">Click a completed step to inspect its data</p>
          <InteractivePipeline
            steps={quote.steps}
            decisionTag={quote.decision_tag}
            emailId={quote.email_id}
            pdfPath={quote.pdf_path}
          />
        </div>
      )}
    </div>
  );
}

// ─── Sidebar ────────────────────────────────────────────────────────────────

const NAV_ITEMS: { key: Page; label: string; icon: JSX.Element }[] = [
  {
    key: "analytics",
    label: "Analytics",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
      </svg>
    ),
  },
  {
    key: "processing",
    label: "Quote Processing",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 12h16.5m-16.5 3.75h16.5M3.75 19.5h16.5M5.625 4.5h12.75a1.875 1.875 0 010 3.75H5.625a1.875 1.875 0 010-3.75z" />
      </svg>
    ),
  },
  {
    key: "placeholder1",
    label: "Underwriter Review",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25z" />
      </svg>
    ),
  },
  {
    key: "quotes",
    label: "Quotes",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
      </svg>
    ),
  },
  {
    key: "email_intake",
    label: "Email Intake",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75" />
      </svg>
    ),
  },
];

function Sidebar({ activePage, onNavigate, darkMode, onToggleTheme }: {
  activePage: Page; onNavigate: (p: Page) => void; darkMode: boolean; onToggleTheme: () => void;
}) {
  return (
    <aside className="w-56 bg-sidebar shrink-0 flex flex-col border-r border-white/5 fixed top-0 left-0 h-full z-40">
      {/* Logo area */}
      <div className="px-4 h-14 flex items-center gap-3 border-b border-white/5">
        <img
          src={darkMode
            ? "https://companieslogo.com/img/orig/databricks.D-0e162e58.png?t=1720244494"
            : "https://images.icon-icons.com/3914/PNG/512/databricks_logo_icon_249070.png"
          }
          alt="Logo"
          className="h-7 w-7 object-contain"
        />
        <span className="text-white/90 font-semibold text-sm tracking-tight">BricksHouse Insurance</span>
      </div>

      {/* Nav items */}
      <nav className="flex-1 py-3 px-2 space-y-0.5">
        {NAV_ITEMS.map((item) => {
          const active = activePage === item.key;
          return (
            <button
              key={item.key}
              onClick={() => onNavigate(item.key)}
              className={`w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
                active
                  ? "bg-white/10 text-white font-medium"
                  : "text-white/50 hover:bg-white/5 hover:text-white/80"
              }`}
            >
              <span className={active ? "text-brick-primary" : "text-white/40"}>{item.icon}</span>
              {item.label}
            </button>
          );
        })}
      </nav>

      {/* Theme toggle + Status */}
      <div className="px-4 py-3 border-t border-white/5 space-y-3">
        <button
          onClick={onToggleTheme}
          className="w-full flex items-center gap-3 px-2 py-1.5 rounded-md text-sm text-white/50 hover:bg-white/5 hover:text-white/80 transition-colors"
        >
          {darkMode ? (
            <svg className="w-4 h-4 text-white/40" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 3v2.25m6.364.386l-1.591 1.591M21 12h-2.25m-.386 6.364l-1.591-1.591M12 18.75V21m-4.773-4.227l-1.591 1.591M5.25 12H3m4.227-4.773L5.636 5.636M15.75 12a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0z" />
            </svg>
          ) : (
            <svg className="w-4 h-4 text-white/40" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M21.752 15.002A9.718 9.718 0 0118 15.75c-5.385 0-9.75-4.365-9.75-9.75 0-1.33.266-2.597.748-3.752A9.753 9.753 0 003 11.25C3 16.635 7.365 21 12.75 21a9.753 9.753 0 009.002-5.998z" />
            </svg>
          )}
          {darkMode ? "Light Mode" : "Dark Mode"}
        </button>
        <div className="flex items-center gap-2">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
          <span className="text-[11px] text-white/40">Live</span>
        </div>
      </div>
    </aside>
  );
}

// ─── Page Header ────────────────────────────────────────────────────────────

function PageHeader({ title, subtitle, right }: {
  title: string; subtitle?: string; right?: React.ReactNode;
}) {
  return (
    <div className="bg-sidebar w-full">
      <div className="px-8 py-5 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-white">{title}</h1>
          {subtitle && <p className="text-sm text-white/40 mt-0.5">{subtitle}</p>}
        </div>
        {right && <div>{right}</div>}
      </div>
      <div className="h-[2px] bg-brick-primary" />
    </div>
  );
}

// ─── Page: Quote Processing ─────────────────────────────────────────────────

const DECISION_FILTERS: { value: string; label: string; color: string }[] = [
  { value: "all", label: "All", color: "bg-white/10 text-white" },
  { value: "auto-approved", label: "Auto-Approved", color: "bg-emerald-500/20 text-emerald-400" },
  { value: "pending-review", label: "Pending Review", color: "bg-amber-500/20 text-amber-400" },
  { value: "auto-declined", label: "Auto-Declined", color: "bg-red-500/20 text-red-400" },
  { value: "uw-approved", label: "UW Approved", color: "bg-emerald-500/20 text-emerald-400" },
  { value: "uw-declined", label: "UW Declined", color: "bg-red-500/20 text-red-400" },
  { value: "uw-info", label: "Info Requested", color: "bg-blue-500/20 text-blue-400" },
  { value: "in-progress", label: "In Progress", color: "bg-cyan-500/20 text-cyan-400" },
];

function QuoteProcessingPage({ quotes, loading, error }: {
  quotes: Quote[]; loading: boolean; error: string | null;
}) {
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [filter, setFilter] = useState("all");

  const filtered = filter === "all"
    ? quotes
    : filter === "in-progress"
      ? quotes.filter((q) => !q.decision_tag)
      : quotes.filter((q) => q.decision_tag === filter);

  return (
    <div>
      <PageHeader
        title="Quote Processing"
        subtitle="Real-time pipeline status for incoming quote requests"
        right={<span className="text-sm text-white/40">{filtered.length} of {quotes.length} quote{quotes.length !== 1 ? "s" : ""}</span>}
      />
      <div className="p-6 max-w-[1200px] mx-auto">

      {/* Decision filter bar */}
      <div className="flex flex-wrap gap-1.5 mb-4">
        {DECISION_FILTERS.map((f) => {
          const count = f.value === "all"
            ? quotes.length
            : f.value === "in-progress"
              ? quotes.filter((q) => !q.decision_tag).length
              : quotes.filter((q) => q.decision_tag === f.value).length;
          const active = filter === f.value;
          return (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                active
                  ? f.color
                  : "bg-white/5 text-white/30 hover:bg-white/10 hover:text-white/50"
              }`}
            >
              {f.label} <span className={active ? "opacity-70" : "opacity-50"}>({count})</span>
            </button>
          );
        })}
      </div>

      {loading && (
        <div className="flex items-center justify-center py-20">
          <div className="w-6 h-6 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
          <span className="ml-3 text-white/40 text-sm">Loading...</span>
        </div>
      )}
      {!loading && error && quotes.length === 0 && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
          <p className="text-red-400 text-sm font-medium">Failed to connect</p>
          <p className="text-red-400/60 text-xs mt-1">{error}</p>
        </div>
      )}
      {!loading && !error && quotes.length === 0 && (
        <div className="bg-[#1e2030] rounded-lg border border-white/5 p-12 text-center">
          <p className="text-white/40 text-sm">No quotes in the pipeline yet</p>
        </div>
      )}
      {!loading && filtered.length === 0 && quotes.length > 0 && (
        <div className="bg-[#1e2030] rounded-lg border border-white/5 p-8 text-center">
          <p className="text-white/40 text-sm">No quotes match this filter</p>
        </div>
      )}

      <div className="space-y-2">
        {filtered.map((q) => (
          <QuoteRow
            key={q.email_id}
            quote={q}
            isExpanded={expandedId === q.email_id}
            onToggle={() => setExpandedId(prev => prev === q.email_id ? null : q.email_id)}
          />
        ))}
      </div>
      </div>
    </div>
  );
}

// ─── Page: Underwriter Review ────────────────────────────────────────────────

interface PendingQuote {
  email_id: string;
  ingestion_timestamp: string;
  business_name: string;
  business_dba: string | null;
  risk_category: string;
  sender_name: string;
  sender_email: string;
  sender_phone: string | null;
  sender_title: string | null;
  annual_revenue: number | null;
  annual_payroll: number | null;
  num_employees: number | null;
  num_locations: number | null;
  coverages_requested: string | null;
  gl_limit_requested: number | null;
  property_tiv: number | null;
  auto_fleet_size: number | null;
  cyber_limit_requested: number | null;
  umbrella_limit_requested: number | null;
  num_claims_5yr: number | null;
  total_claims_amount: number | null;
  worst_claim_description: string | null;
  current_carrier: string | null;
  current_premium: number | null;
  special_requirements: string | null;
  urgency: string | null;
  risk_score: number | null;
  risk_band: string | null;
  predicted_loss_ratio: number | null;
  pricing_action: string | null;
  review_summary: string | null;
  decision_tag: string;
  claim_prediction: number | null;
  scoring_method: string | null;
  review_timestamp: string | null;
  uw_decision: string | null;
  uw_decided_at: string | null;
}

function UnderwriterReviewPage() {
  const [pending, setPending] = useState<PendingQuote[]>([]);
  const [selected, setSelected] = useState<PendingQuote | null>(null);
  const [action, setAction] = useState<"approve" | "decline" | "info" | null>(null);
  const [surcharge, setSurcharge] = useState("");
  const [discount, setDiscount] = useState("");
  const [notes, setNotes] = useState("");
  const [infoRequest, setInfoRequest] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState<string | null>(null);
  const [chatOpen, setChatOpen] = useState(false);
  const [chatInput, setChatInput] = useState("");
  const [chatHistory, setChatHistory] = useState<{ role: "user" | "assistant"; text: string }[]>([]);
  const [chatLoading, setChatLoading] = useState(false);

  const fetchPending = useCallback(async () => {
    try {
      const r = await fetch("/api/underwriter/pending");
      if (r.ok) setPending(await r.json());
    } catch { /* silent */ }
  }, []);

  useEffect(() => {
    fetchPending();
    const t = setInterval(fetchPending, 5000);
    return () => clearInterval(t);
  }, [fetchPending]);

  const resetForm = () => {
    setAction(null); setSurcharge(""); setDiscount(""); setNotes(""); setInfoRequest("");
  };

  const handleSelect = (q: PendingQuote) => {
    setSelected(q); resetForm(); setSubmitted(null);
    setChatOpen(false); setChatHistory([]); setChatInput("");
  };

  const sendChat = async () => {
    if (!selected || !chatInput.trim()) return;
    const question = chatInput.trim();
    setChatHistory(prev => [...prev, { role: "user", text: question }]);
    setChatInput(""); setChatLoading(true);
    try {
      const r = await fetch("/api/underwriter/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email_id: selected.email_id, question }),
      });
      const d = await r.json();
      setChatHistory(prev => [...prev, { role: "assistant", text: d.answer || "No response" }]);
    } catch {
      setChatHistory(prev => [...prev, { role: "assistant", text: "Failed to get a response. Please try again." }]);
    } finally { setChatLoading(false); }
  };

  const handleSubmit = async () => {
    if (!selected || !action) return;
    setSubmitting(true);
    const decision = action === "approve" ? "uw-approved" : action === "decline" ? "uw-declined" : "uw-info";
    try {
      const r = await fetch("/api/underwriter/decide", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email_id: selected.email_id, decision,
          surcharge_pct: parseFloat(surcharge) || 0,
          discount_pct: parseFloat(discount) || 0,
          notes, info_request: infoRequest,
        }),
      });
      if (r.ok) {
        setSubmitted(decision);
        fetchPending();
      }
    } catch { /* silent */ } finally { setSubmitting(false); }
  };

  // Filter out already-decided quotes
  const queue = pending.filter(q => !q.uw_decision);

  return (
    <div>
      <PageHeader
        title="Underwriter Review"
        subtitle="Quotes requiring manual underwriting review"
        right={<span className="text-sm text-white/40">{queue.length} pending</span>}
      />
      <div className="p-6">
      <div className="flex gap-6 min-h-[calc(100vh-180px)]">
        {/* Left: Quote list */}
        <div className="w-80 shrink-0 space-y-1.5">
          {queue.length === 0 && (
            <div className="bg-[#1e2030] rounded-lg border border-white/5 p-8 text-center">
              <p className="text-sm text-white/40">No quotes pending review</p>
            </div>
          )}
          {queue.map((q) => (
            <button
              key={q.email_id}
              onClick={() => handleSelect(q)}
              className={`w-full text-left px-4 py-3 rounded-lg border transition-colors ${
                selected?.email_id === q.email_id
                  ? "border-brick-primary bg-brick-primary/10"
                  : "border-white/5 bg-[#1e2030] hover:border-white/10"
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-sm font-semibold text-white truncate">{q.business_name}</span>
                <RiskBadge band={q.risk_band} />
              </div>
              <p className="text-[11px] text-white/40 mt-0.5 truncate">{q.sender_name} &middot; {q.sender_email}</p>
              <div className="flex items-center justify-between mt-1.5">
                <span className="text-[11px] text-white/40">{relativeTime(q.ingestion_timestamp)}</span>
                <span className="text-[11px] font-medium text-white/50">Score: {q.risk_score?.toFixed(0)}</span>
              </div>
            </button>
          ))}
        </div>

        {/* Right: Detail panel */}
        <div className="flex-1 min-w-0">
          {!selected ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-white/40">Select a quote to review</p>
            </div>
          ) : submitted ? (
            <div className="bg-[#1e2030] rounded-lg border border-white/5 p-8 text-center expand-enter">
              <div className={`w-14 h-14 rounded-full flex items-center justify-center mx-auto mb-4 ${
                submitted === "uw-approved" ? "bg-emerald-500/10" : submitted === "uw-declined" ? "bg-red-500/10" : "bg-amber-500/10"
              }`}>
                {submitted === "uw-approved" && (
                  <svg className="w-7 h-7 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                  </svg>
                )}
                {submitted === "uw-declined" && (
                  <svg className="w-7 h-7 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                )}
                {submitted === "uw-info" && (
                  <svg className="w-7 h-7 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75" />
                  </svg>
                )}
              </div>
              <h3 className="text-lg font-semibold text-white">
                {submitted === "uw-approved" ? "Quote Approved" : submitted === "uw-declined" ? "Quote Declined" : "Information Requested"}
              </h3>
              <p className="text-sm text-white/40 mt-1">{selected.business_name}</p>
              <button onClick={() => { setSelected(null); setSubmitted(null); }}
                className="mt-4 text-sm text-brick-primary hover:underline">Back to queue</button>
            </div>
          ) : (
            <div className="bg-[#1e2030] rounded-lg border border-white/5 overflow-hidden expand-enter">
              {/* Quote header */}
              <div className="px-6 py-4 border-b border-white/5">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-white">{selected.business_name}</h2>
                    <p className="text-xs text-white/40 mt-0.5">
                      {selected.sender_name} &middot; {selected.sender_email}
                      {selected.sender_phone && ` &middot; ${selected.sender_phone}`}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <RiskBadge band={selected.risk_band} />
                    {selected.urgency && <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-brick-primary/10 text-brick-primary">{selected.urgency}</span>}
                  </div>
                </div>
              </div>

              {/* Review narrative */}
              <div className="px-6 py-4 border-b border-white/5 bg-amber-500/10">
                <h4 className="text-xs font-semibold text-amber-400 uppercase tracking-wider mb-2">Why This Requires Review</h4>
                <p className="text-sm text-white/70 leading-relaxed">{selected.review_summary || "No review summary available."}</p>
              </div>

              {/* Details grid */}
              <div className="px-6 py-4 border-b border-white/5">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  {[
                    ["Risk Score", selected.risk_score != null ? `${selected.risk_score.toFixed(1)} / 100` : "--"],
                    ["Loss Ratio", selected.predicted_loss_ratio != null ? selected.predicted_loss_ratio.toFixed(2) : "--"],
                    ["Revenue", formatCurrency(selected.annual_revenue)],
                    ["Employees", selected.num_employees?.toLocaleString() ?? "--"],
                    ["Locations", selected.num_locations?.toString() ?? "--"],
                    ["Claims (5yr)", selected.num_claims_5yr?.toString() ?? "--"],
                    ["Claims Amount", formatCurrency(selected.total_claims_amount)],
                    ["Current Premium", formatCurrency(selected.current_premium)],
                    ["Current Carrier", selected.current_carrier ?? "--"],
                    ["Pricing Action", selected.pricing_action?.replace(/_/g, " ") ?? "--"],
                    ["Coverages", selected.coverages_requested ?? "--"],
                    ["Category", selected.risk_category?.replace(/_/g, " ") ?? "--"],
                  ].map(([label, val]) => (
                    <div key={label as string} className={`${(label as string) === "Coverages" ? "lg:col-span-2" : ""}`}>
                      <p className="text-[10px] text-white/40 uppercase tracking-wider">{label as string}</p>
                      <p className="text-sm font-medium text-white/80 mt-0.5">{val as string}</p>
                    </div>
                  ))}
                </div>
                {selected.worst_claim_description && (
                  <div className="mt-3 pt-3 border-t border-white/5">
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Worst Claim</p>
                    <p className="text-sm text-white/70 mt-0.5">{selected.worst_claim_description}</p>
                  </div>
                )}
                {selected.special_requirements && (
                  <div className="mt-2">
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Special Requirements</p>
                    <p className="text-sm text-white/70 mt-0.5">{selected.special_requirements}</p>
                  </div>
                )}
              </div>

              {/* Decision section */}
              <div className="px-6 py-5">
                <h4 className="text-xs font-semibold text-white/50 uppercase tracking-wider mb-3">Underwriter Decision</h4>

                {/* Action buttons */}
                <div className="flex gap-2 mb-4">
                  <button onClick={() => { setAction("approve"); setDiscount(""); setSurcharge(""); }}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "approve"
                        ? "bg-emerald-500 text-white border-emerald-500"
                        : "bg-white/5 text-white/70 border-white/10 hover:border-emerald-500/40 hover:text-emerald-400"
                    }`}>Approve</button>
                  <button onClick={() => setAction("decline")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "decline"
                        ? "bg-red-500 text-white border-red-500"
                        : "bg-white/5 text-white/70 border-white/10 hover:border-red-500/40 hover:text-red-400"
                    }`}>Decline</button>
                  <button onClick={() => setAction("info")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "info"
                        ? "bg-amber-500 text-white border-amber-500"
                        : "bg-white/5 text-white/70 border-white/10 hover:border-amber-500/40 hover:text-amber-400"
                    }`}>Request Info</button>
                </div>

                {/* Approve options */}
                {action === "approve" && (
                  <div className="expand-enter space-y-3 mb-4">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="text-[11px] text-white/50 uppercase tracking-wider">Surcharge %</label>
                        <input type="number" min="0" max="100" step="0.5" value={surcharge} onChange={e => setSurcharge(e.target.value)}
                          placeholder="0" className="mt-1 w-full px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary" />
                      </div>
                      <div>
                        <label className="text-[11px] text-white/50 uppercase tracking-wider">Discount %</label>
                        <input type="number" min="0" max="100" step="0.5" value={discount} onChange={e => setDiscount(e.target.value)}
                          placeholder="0" className="mt-1 w-full px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary" />
                      </div>
                    </div>
                  </div>
                )}

                {/* Request info */}
                {action === "info" && (
                  <div className="expand-enter mb-4">
                    <label className="text-[11px] text-white/50 uppercase tracking-wider">Information Required</label>
                    <textarea value={infoRequest} onChange={e => setInfoRequest(e.target.value)}
                      rows={3} placeholder="Describe the additional information needed from the applicant..."
                      className="mt-1 w-full px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary resize-none" />
                  </div>
                )}

                {/* Notes (always visible when action selected) */}
                {action && (
                  <div className="expand-enter mb-4">
                    <label className="text-[11px] text-white/50 uppercase tracking-wider">Underwriter Notes</label>
                    <textarea value={notes} onChange={e => setNotes(e.target.value)}
                      rows={2} placeholder="Add notes about your decision..."
                      className="mt-1 w-full px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary resize-none" />
                  </div>
                )}

                {/* Submit */}
                {action && (
                  <button onClick={handleSubmit} disabled={submitting || (action === "info" && !infoRequest.trim())}
                    className="w-full py-2.5 rounded-lg text-sm font-medium text-white bg-brick-primary hover:bg-brick-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                    {submitting ? "Submitting..." : action === "approve" ? "Confirm Approval" : action === "decline" ? "Confirm Decline" : "Send Information Request"}
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Floating AI Assistant button + popup */}
      {selected && !submitted && (
        <>
          {/* Chat popup */}
          {chatOpen && (
            <div className="fixed bottom-20 right-8 w-[420px] max-h-[520px] bg-[#1e2030] rounded-xl shadow-2xl border border-white/10 flex flex-col z-50 expand-enter">
              {/* Header */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-white/5 bg-sidebar rounded-t-xl">
                <div className="flex items-center gap-2">
                  <div className="w-6 h-6 rounded-full bg-brick-primary/20 flex items-center justify-center">
                    <svg className="w-3.5 h-3.5 text-brick-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
                    </svg>
                  </div>
                  <span className="text-sm font-medium text-white">AI Quote Assistant</span>
                </div>
                <button onClick={() => setChatOpen(false)} className="text-white/40 hover:text-white/80 transition-colors">
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              {/* Chat body */}
              <div className="flex-1 overflow-y-auto p-4 space-y-3 min-h-[200px] max-h-[340px]">
                {chatHistory.length === 0 && (
                  <div className="text-center py-8">
                    <div className="w-10 h-10 rounded-full bg-white/5 flex items-center justify-center mx-auto mb-3">
                      <svg className="w-5 h-5 text-white/20" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                      </svg>
                    </div>
                    <p className="text-xs text-white/40">Ask anything about <span className="font-medium text-white/60">{selected.business_name}</span></p>
                    <div className="mt-3 flex flex-wrap gap-1.5 justify-center">
                      {["What are the main risk factors?", "Summarize the claims history", "Is the premium reasonable?"].map(q => (
                        <button key={q} onClick={() => { setChatInput(q); }}
                          className="text-[11px] px-2.5 py-1 bg-white/5 hover:bg-white/10 text-white/50 rounded-full transition-colors">{q}</button>
                      ))}
                    </div>
                  </div>
                )}
                {chatHistory.map((msg, i) => (
                  <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                    <div className={`max-w-[85%] px-3 py-2 rounded-lg text-sm leading-relaxed ${
                      msg.role === "user"
                        ? "bg-brick-primary text-white rounded-br-sm"
                        : "bg-white/5 text-white/70 border border-white/10 rounded-bl-sm"
                    }`}>
                      {msg.role === "assistant" ? (
                        <div className="whitespace-pre-wrap">{msg.text}</div>
                      ) : msg.text}
                    </div>
                  </div>
                ))}
                {chatLoading && (
                  <div className="flex justify-start">
                    <div className="px-3 py-2 bg-white/5 border border-white/10 rounded-lg rounded-bl-sm flex items-center gap-2">
                      <div className="w-3.5 h-3.5 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
                      <span className="text-xs text-white/40">Thinking...</span>
                    </div>
                  </div>
                )}
              </div>

              {/* Input */}
              <div className="px-3 py-3 border-t border-white/5">
                <div className="flex gap-2">
                  <input
                    value={chatInput}
                    onChange={e => setChatInput(e.target.value)}
                    onKeyDown={e => e.key === "Enter" && !e.shiftKey && sendChat()}
                    placeholder="Ask about this quote..."
                    disabled={chatLoading}
                    className="flex-1 px-3 py-2 bg-white/5 border border-white/10 rounded-lg text-sm text-white placeholder-white/30 focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary disabled:opacity-50"
                  />
                  <button onClick={sendChat} disabled={chatLoading || !chatInput.trim()}
                    className="px-3 py-2 bg-brick-primary text-white rounded-lg hover:bg-brick-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 12L3.269 3.126A59.768 59.768 0 0121.485 12 59.77 59.77 0 013.27 20.876L5.999 12zm0 0h7.5" />
                    </svg>
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Floating button */}
          <button
            onClick={() => setChatOpen(prev => !prev)}
            className={`fixed bottom-6 right-8 w-12 h-12 rounded-full shadow-lg flex items-center justify-center transition-all z-50 hover:scale-110 active:scale-95 ${
              chatOpen ? "bg-sidebar text-white" : "bg-brick-primary text-white"
            }`}
            title="Ask AI about this quote"
          >
            {chatOpen ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.879 7.519c1.171-1.025 3.071-1.025 4.242 0 1.172 1.025 1.172 2.687 0 3.712-.203.179-.43.326-.67.442-.745.361-1.45.999-1.45 1.827v.75M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9 5.25h.008v.008H12v-.008z" />
              </svg>
            )}
          </button>
        </>
      )}
      </div>
    </div>
  );
}

// ─── Risk category colors ────────────────────────────────────────────────────

const RISK_CAT_COLORS: Record<string, string> = {
  office: "bg-blue-500/10 text-blue-400",
  retail: "bg-purple-500/10 text-purple-400",
  construction: "bg-orange-500/10 text-orange-400",
  manufacturing: "bg-slate-500/10 text-slate-300",
  healthcare: "bg-rose-500/10 text-rose-400",
  technology: "bg-cyan-500/10 text-cyan-400",
  food_service: "bg-yellow-500/10 text-yellow-400",
  transportation: "bg-indigo-500/10 text-indigo-400",
  professional_services: "bg-teal-500/10 text-teal-400",
  hospitality: "bg-pink-500/10 text-pink-400",
};

const RISK_LEVEL_COLORS: Record<string, string> = {
  low: "bg-emerald-500/10 text-emerald-400",
  medium: "bg-amber-500/10 text-amber-400",
  high: "bg-red-500/10 text-red-400",
};

// ─── Page: Email Intake ──────────────────────────────────────────────────────

function EmailIntakePage() {
  const [samples, setSamples] = useState<SampleEmail[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string>("");
  const [emlContent, setEmlContent] = useState("");
  const [sending, setSending] = useState(false);
  const [sent, setSent] = useState<{ file_name: string } | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterRisk, setFilterRisk] = useState<string>("all");
  const [loadingEml, setLoadingEml] = useState(false);

  // Fetch sample emails on mount
  useEffect(() => {
    (async () => {
      try {
        const r = await fetch("/api/sample-emails");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        setSamples(d.emails || []);
        setError(null);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load samples");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  // Filter samples
  const filtered = samples.filter((s) => {
    const matchSearch =
      !searchTerm ||
      s.label.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.business_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.sender_name.toLowerCase().includes(searchTerm.toLowerCase());
    const matchRisk = filterRisk === "all" || s.risk_level === filterRisk;
    return matchSearch && matchRisk;
  });

  // Get unique risk levels
  const riskLevels = [...new Set(samples.map((s) => s.risk_level))].sort();

  // Select a sample — lazy-load eml_content
  const handleSelect = async (sample: SampleEmail) => {
    setSelectedId(sample.org_id);
    setEmlContent("");
    setSent(null);
    setLoadingEml(true);
    try {
      const r = await fetch(`/api/sample-emails/${sample.org_id}/eml`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setEmlContent(d.eml_content || "");
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to load email content");
    } finally {
      setLoadingEml(false);
    }
  };

  // Send email
  const handleSend = async () => {
    if (!emlContent.trim()) return;
    setSending(true);
    setSent(null);
    try {
      const r = await fetch("/api/send-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ eml_content: emlContent }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setSent({ file_name: d.file_name });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Send failed");
    } finally {
      setSending(false);
    }
  };

  const selected = samples.find((s) => s.org_id === selectedId);

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="Email Intake"
        subtitle="Simulate incoming insurance quote request emails"
        right={
          <div className="flex items-center gap-3">
            <span className="text-xs text-white/40">{samples.length} sample emails</span>
            <span className="text-xs text-white/40">|</span>
            <span className="text-xs text-white/40">{riskLevels.length} risk levels</span>
          </div>
        }
      />

      {/* Error banner */}
      {error && (
        <div className="mx-6 mt-4 px-4 py-2 bg-red-500/10 border border-red-500/20 rounded-lg text-sm text-red-400">
          {error}
          <button className="ml-2 underline" onClick={() => setError(null)}>dismiss</button>
        </div>
      )}

      <div className="flex flex-1 min-h-0 p-6 gap-6">
        {/* ── Left panel: Company list ── */}
        <div className="w-[380px] shrink-0 flex flex-col bg-[#1e2030] border border-white/5 rounded-xl overflow-hidden">
          {/* Search */}
          <div className="p-3 border-b border-white/5">
            <input
              type="text"
              placeholder="Search companies..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-white/5 border border-white/10 rounded-lg text-white placeholder-white/30 focus:outline-none focus:ring-1 focus:ring-brick-primary focus:border-brick-primary"
            />
          </div>

          {/* Risk level filter pills */}
          <div className="flex flex-wrap gap-1 px-3 py-2 border-b border-white/5">
            <button
              onClick={() => setFilterRisk("all")}
              className={`px-2 py-0.5 rounded text-[10px] font-medium transition-colors ${
                filterRisk === "all"
                  ? "bg-white/20 text-white"
                  : "bg-white/5 text-white/50 hover:bg-white/10"
              }`}
            >
              All
            </button>
            {riskLevels.map((level) => (
              <button
                key={level}
                onClick={() => setFilterRisk(level)}
                className={`px-2 py-0.5 rounded text-[10px] font-medium transition-colors capitalize ${
                  filterRisk === level
                    ? RISK_LEVEL_COLORS[level] || "bg-white/10 text-white/60"
                    : "bg-white/5 text-white/40 hover:bg-white/10"
                }`}
              >
                {level} risk
              </button>
            ))}
          </div>

          {/* Scrollable list */}
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-12 text-sm text-white/40">
                <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full step-spinner mr-2" />
                Loading...
              </div>
            ) : filtered.length === 0 ? (
              <div className="py-12 text-center text-sm text-white/40">No matching companies</div>
            ) : (
              filtered.map((s) => (
                <button
                  key={s.org_id}
                  onClick={() => handleSelect(s)}
                  className={`w-full text-left px-4 py-3 border-b border-white/5 transition-colors ${
                    s.org_id === selectedId
                      ? "bg-brick-primary/10 border-l-2 border-l-brick-primary"
                      : "hover:bg-white/5 border-l-2 border-l-transparent"
                  }`}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-sm font-medium text-white/90 truncate flex-1">
                      {s.business_name}
                    </span>
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-semibold shrink-0 ${RISK_LEVEL_COLORS[s.risk_level] || "bg-white/10 text-white/50"}`}>
                      {s.risk_level}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className={`px-1.5 py-0.5 rounded text-[9px] font-medium shrink-0 ${RISK_CAT_COLORS[s.risk_category] || "bg-white/10 text-white/50"}`}>
                      {(s.risk_category || "").replace("_", " ")}
                    </span>
                    <span className="text-[11px] text-white/40 truncate">
                      {s.sender_name} · ${(s.annual_revenue || 0).toLocaleString()}
                    </span>
                  </div>
                </button>
              ))
            )}
          </div>

          {/* List footer */}
          <div className="px-3 py-2 border-t border-white/5 text-[11px] text-white/40 text-center">
            {filtered.length} of {samples.length} companies
          </div>
        </div>

        {/* ── Right panel: Email editor ── */}
        <div className="flex-1 flex flex-col min-w-0">
          {!selectedId && !emlContent ? (
            <div className="flex-1 flex items-center justify-center bg-[#1e2030] border border-white/5 rounded-xl">
              <div className="text-center text-white/40">
                <svg className="w-12 h-12 mx-auto mb-3 text-white/20" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75" />
                </svg>
                <p className="text-sm font-medium text-white/60">Select a company</p>
                <p className="text-xs mt-1 text-white/40">Click a quote request on the left to preview the email</p>
              </div>
            </div>
          ) : (
            <div className="flex-1 flex flex-col bg-[#1e2030] border border-white/5 rounded-xl overflow-hidden">
              {/* Email header */}
              {emlContent && (
                <div className="bg-[#13141f] border-b border-white/5 px-5 py-3 space-y-1 text-sm shrink-0">
                  {emlContent.split("\n").slice(0, 7).map((line, i) => {
                    const [key, ...rest] = line.split(": ");
                    const val = rest.join(": ");
                    if (!val) return null;
                    return (
                      <div key={i} className="flex gap-2">
                        <span className="text-white/40 font-medium w-20 shrink-0">{key}:</span>
                        <span className="text-white/70 truncate">{val}</span>
                      </div>
                    );
                  })}
                </div>
              )}

              {/* Email body */}
              {loadingEml ? (
                <div className="flex-1 flex items-center justify-center text-sm text-white/40">
                  <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full step-spinner mr-2" />
                  Loading email...
                </div>
              ) : (
                <textarea
                  value={emlContent}
                  onChange={(e) => { setEmlContent(e.target.value); setSent(null); }}
                  placeholder="Select a company on the left, or paste/type an email here..."
                  className="flex-1 px-5 py-4 text-sm font-mono text-white/70 bg-transparent placeholder-white/20 focus:outline-none resize-none"
                />
              )}

              {/* Send bar */}
              <div className="shrink-0 px-5 py-3 border-t border-white/5 bg-[#13141f] flex items-center gap-4">
                <button
                  onClick={handleSend}
                  disabled={!emlContent.trim() || sending}
                  className={`px-5 py-2 rounded-lg text-sm font-semibold transition-all flex items-center gap-2 ${
                    !emlContent.trim() || sending
                      ? "bg-white/10 text-white/30 cursor-not-allowed"
                      : "bg-brick-primary text-white hover:bg-red-600 shadow-sm hover:shadow"
                  }`}
                >
                  {sending ? (
                    <>
                      <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full step-spinner" />
                      Sending...
                    </>
                  ) : (
                    <>
                      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M6 12L3.269 3.126A59.768 59.768 0 0121.485 12 59.77 59.77 0 013.27 20.876L5.999 12zm0 0h7.5" />
                      </svg>
                      Send to Pipeline
                    </>
                  )}
                </button>

                {sent && (
                  <div className="flex items-center gap-2 px-3 py-1.5 bg-emerald-500/10 border border-emerald-500/20 rounded-lg">
                    <svg className="w-4 h-4 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <span className="text-sm text-emerald-400">
                      Sent as <span className="font-mono text-xs">{sent.file_name}</span>
                    </span>
                  </div>
                )}

                <div className="ml-auto text-[11px] text-white/40">
                  Email is saved to the incoming volume for pipeline processing
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Page: Analytics ─────────────────────────────────────────────────────────

interface AnalyticsSummary {
  total_quotes: number;
  completed_quotes: number;
  auto_approved: number;
  auto_declined: number;
  pending_review: number;
  uw_approved: number;
  uw_declined: number;
  avg_completion_seconds: number | null;
  avg_automated_seconds: number | null;
  avg_uw_delay_seconds: number | null;
}

interface ResponseDelayPoint {
  email_id: string;
  business_name: string | null;
  ingestion_timestamp: string | null;
  completed_timestamp: string | null;
  delay_seconds: number | null;
}

interface CategoryStat {
  risk_category: string;
  count: number;
  avg_risk_score: number | null;
  avg_premium: number | null;
}

function StatCard({ label, value, sub, color }: { label: string; value: string | number; sub?: string; color?: string }) {
  return (
    <div className="bg-[#1e2030] rounded-xl border border-white/5 px-5 py-4">
      <p className="text-[11px] text-white/40 uppercase tracking-wider">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${color || "text-white"}`}>{value}</p>
      {sub && <p className="text-xs text-white/30 mt-0.5">{sub}</p>}
    </div>
  );
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return "--";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.round((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

const CAT_COLORS: Record<string, string> = {
  office: "#3b82f6", retail: "#8b5cf6", construction: "#f97316",
  manufacturing: "#64748b", healthcare: "#f43f5e", technology: "#06b6d4",
  food_service: "#eab308", transportation: "#6366f1", professional_services: "#14b8a6",
  hospitality: "#ec4899", unknown: "#9ca3af",
};

// ─── SVG Line Chart (no external deps) ──────────────────────────────────────

function SvgLineChart({ data }: { data: { name: string; value: number; label: string }[] }) {
  const [hovered, setHovered] = useState<number | null>(null);
  if (data.length === 0) return null;

  const W = 900, H = 320;
  const pad = { top: 20, right: 30, bottom: 70, left: 60 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  const values = data.map(d => d.value);
  const maxV = Math.max(...values, 1);
  const minV = Math.min(...values, 0);
  const range = maxV - minV || 1;

  const x = (i: number) => pad.left + (i / Math.max(data.length - 1, 1)) * cw;
  const y = (v: number) => pad.top + ch - ((v - minV) / range) * ch;

  const gridCount = 5;
  const gridLines = Array.from({ length: gridCount + 1 }, (_, i) => {
    const val = minV + (range / gridCount) * i;
    return { val, yPos: y(val) };
  });

  const pathD = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(d.value).toFixed(1)}`).join(" ");

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 320 }}>
      {/* Grid */}
      {gridLines.map((g, i) => (
        <g key={i}>
          <line x1={pad.left} y1={g.yPos} x2={W - pad.right} y2={g.yPos} stroke="rgba(255,255,255,0.06)" />
          <text x={pad.left - 8} y={g.yPos + 4} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize={10}>
            {g.val.toFixed(1)}
          </text>
        </g>
      ))}

      {/* Y axis label */}
      <text x={14} y={H / 2} textAnchor="middle" fill="rgba(255,255,255,0.3)" fontSize={10} transform={`rotate(-90, 14, ${H / 2})`}>
        Seconds
      </text>

      {/* Line */}
      <path d={pathD} fill="none" stroke="#2dd4bf" strokeWidth={2.5} strokeLinejoin="round" strokeLinecap="round" />

      {/* Area fill */}
      <path
        d={`${pathD} L${x(data.length - 1).toFixed(1)},${(pad.top + ch).toFixed(1)} L${pad.left.toFixed(1)},${(pad.top + ch).toFixed(1)} Z`}
        fill="url(#areaGrad)" opacity={0.2}
      />
      <defs>
        <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#2dd4bf" />
          <stop offset="100%" stopColor="#2dd4bf" stopOpacity={0} />
        </linearGradient>
      </defs>

      {/* Dots + labels */}
      {data.map((d, i) => (
        <g key={i} onMouseEnter={() => setHovered(i)} onMouseLeave={() => setHovered(null)} style={{ cursor: "pointer" }}>
          <circle cx={x(i)} cy={y(d.value)} r={hovered === i ? 6 : 4} fill="#2dd4bf" stroke="#1e2030" strokeWidth={2} />
          {/* X labels */}
          <text
            x={x(i)} y={pad.top + ch + 16} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize={9}
            transform={`rotate(-35, ${x(i)}, ${pad.top + ch + 16})`}
          >
            {d.name.length > 14 ? d.name.slice(0, 14) + "..." : d.name}
          </text>
          {/* Tooltip on hover */}
          {hovered === i && (
            <g>
              <rect x={x(i) - 55} y={y(d.value) - 36} width={110} height={28} rx={6} fill="#282a3a" stroke="rgba(255,255,255,0.1)" />
              <text x={x(i)} y={y(d.value) - 18} textAnchor="middle" fill="#2dd4bf" fontSize={11} fontWeight={600}>
                {d.value}s
              </text>
            </g>
          )}
        </g>
      ))}
    </svg>
  );
}

// ─── SVG Bar Chart (no external deps) ───────────────────────────────────────

function SvgBarChart({ data }: { data: { label: string; value: number; color: string }[] }) {
  const [hovered, setHovered] = useState<number | null>(null);
  if (data.length === 0) return null;

  const W = 900, H = 280;
  const pad = { top: 20, right: 30, bottom: 60, left: 50 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  const maxV = Math.max(...data.map(d => d.value), 1);
  const barW = Math.min(cw / data.length * 0.6, 60);
  const gap = cw / data.length;

  const gridCount = 4;
  const gridLines = Array.from({ length: gridCount + 1 }, (_, i) => {
    const val = (maxV / gridCount) * i;
    return { val, yPos: pad.top + ch - (val / maxV) * ch };
  });

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 280 }}>
      {/* Grid */}
      {gridLines.map((g, i) => (
        <g key={i}>
          <line x1={pad.left} y1={g.yPos} x2={W - pad.right} y2={g.yPos} stroke="rgba(255,255,255,0.06)" />
          <text x={pad.left - 8} y={g.yPos + 4} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize={10}>
            {Math.round(g.val)}
          </text>
        </g>
      ))}

      {/* Bars */}
      {data.map((d, i) => {
        const bx = pad.left + gap * i + (gap - barW) / 2;
        const bh = (d.value / maxV) * ch;
        const by = pad.top + ch - bh;
        return (
          <g key={i} onMouseEnter={() => setHovered(i)} onMouseLeave={() => setHovered(null)} style={{ cursor: "pointer" }}>
            <rect x={bx} y={by} width={barW} height={bh} rx={4} fill={d.color} opacity={hovered === i ? 1 : 0.7} />
            <text
              x={bx + barW / 2} y={pad.top + ch + 16} textAnchor="end" fill="rgba(255,255,255,0.3)" fontSize={9}
              transform={`rotate(-25, ${bx + barW / 2}, ${pad.top + ch + 16})`}
            >
              {d.label}
            </text>
            {hovered === i && (
              <g>
                <rect x={bx + barW / 2 - 30} y={by - 28} width={60} height={22} rx={5} fill="#282a3a" stroke="rgba(255,255,255,0.1)" />
                <text x={bx + barW / 2} y={by - 12} textAnchor="middle" fill="white" fontSize={11} fontWeight={600}>
                  {d.value}
                </text>
              </g>
            )}
          </g>
        );
      })}
    </svg>
  );
}

function AnalyticsPage() {
  const [summary, setSummary] = useState<AnalyticsSummary | null>(null);
  const [delayData, setDelayData] = useState<ResponseDelayPoint[]>([]);
  const [categoryData, setCategoryData] = useState<CategoryStat[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnalytics = async () => {
      try {
        const r = await fetch("/api/analytics");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        setSummary(d.summary || null);
        setDelayData(d.response_delay || []);
        setCategoryData(d.by_category || []);
        setError(null);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load analytics");
      } finally {
        setLoading(false);
      }
    };
    fetchAnalytics();
    const t = setInterval(fetchAnalytics, 10000);
    return () => clearInterval(t);
  }, []);

  const chartData = delayData.map((d) => ({
    name: d.business_name || d.email_id.slice(0, 8),
    value: d.delay_seconds != null ? Math.round(d.delay_seconds) : 0,
    label: d.ingestion_timestamp ? new Date(d.ingestion_timestamp).toLocaleDateString() : "",
  }));

  const barData = categoryData.map((c) => ({
    label: c.risk_category.replace(/_/g, " "),
    value: c.count,
    color: CAT_COLORS[c.risk_category] || CAT_COLORS.unknown,
  }));

  const pct = (n: number) => summary && summary.total_quotes > 0
    ? Math.round((n / summary.total_quotes) * 100) : 0;
  const completionRate = pct(summary?.completed_quotes ?? 0);

  return (
    <div className="bg-[#13141f] min-h-full">
      <PageHeader title="Analytics for Email to Quote Automation" subtitle="Pipeline metrics and performance dashboards" />
      <div className="p-6 max-w-[1200px] mx-auto space-y-6">
        {loading && (
          <div className="flex items-center justify-center py-20">
            <div className="w-6 h-6 border-2 border-teal-400 border-t-transparent rounded-full step-spinner" />
            <span className="ml-3 text-white/40 text-sm">Loading analytics...</span>
          </div>
        )}
        {!loading && error && (
          <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
            <p className="text-red-400 text-sm font-medium">Failed to load analytics</p>
            <p className="text-red-400/60 text-xs mt-1">{error}</p>
          </div>
        )}
        {!loading && summary && (
          <>
            <div className="space-y-3">
            {/* Quote Stats */}
            <div className="flex rounded-xl border border-white/10 overflow-hidden">
              <div className="flex items-center justify-center w-10 min-w-[2.5rem] bg-white/5 border-r border-white/10">
                <span className="text-[10px] font-bold text-white/50 uppercase tracking-widest whitespace-nowrap" style={{ writingMode: "vertical-rl", transform: "rotate(180deg)" }}>Quotes</span>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 flex-1 p-4">
                <StatCard label="Total Quotes" value={summary.total_quotes} />
                <StatCard label="Completed" value={summary.completed_quotes} sub={`${completionRate}% of total`} color="text-teal-400" />
                <StatCard label="Pending Review" value={summary.pending_review} sub={`${pct(summary.pending_review)}% of total`} color="text-amber-400" />
                <StatCard label="Avg Processing Time" value={formatDuration(summary.avg_completion_seconds)} sub="All quotes incl. pending" color="text-white" />
              </div>
            </div>

            {/* Automated */}
            <div className="flex rounded-xl border border-white/10 overflow-hidden">
              <div className="flex items-center justify-center w-10 min-w-[2.5rem] bg-white/5 border-r border-white/10">
                <span className="text-[10px] font-bold text-white/50 uppercase tracking-widest whitespace-nowrap" style={{ writingMode: "vertical-rl", transform: "rotate(180deg)" }}>Automated</span>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 flex-1 p-4">
                <StatCard label="Auto-Approved" value={summary.auto_approved} sub={`${pct(summary.auto_approved)}% of total`} color="text-emerald-400" />
                <StatCard label="Auto-Declined" value={summary.auto_declined} sub={`${pct(summary.auto_declined)}% of total`} color="text-red-400" />
                <StatCard label="Automated Processing" value={summary.auto_approved + summary.auto_declined} sub={`${pct(summary.auto_approved + summary.auto_declined)}% of total`} color="text-blue-400" />
                <StatCard label="Avg Automated Delay" value={formatDuration(summary.avg_automated_seconds)} sub="Auto-approved &amp; declined" color="text-cyan-400" />
              </div>
            </div>

            {/* Underwriter */}
            <div className="flex rounded-xl border border-white/10 overflow-hidden">
              <div className="flex items-center justify-center w-10 min-w-[2.5rem] bg-white/5 border-r border-white/10">
                <span className="text-[10px] font-bold text-white/50 uppercase tracking-widest whitespace-nowrap" style={{ writingMode: "vertical-rl", transform: "rotate(180deg)" }}>Underwriter</span>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 flex-1 p-4">
                <StatCard label="UW Approved" value={summary.uw_approved} sub={`${pct(summary.uw_approved)}% of total`} color="text-emerald-400" />
                <StatCard label="UW Declined" value={summary.uw_declined} sub={`${pct(summary.uw_declined)}% of total`} color="text-red-400" />
                <StatCard label="Underwriter Processing" value={summary.uw_approved + summary.uw_declined} sub={`${pct(summary.uw_approved + summary.uw_declined)}% of total`} color="text-violet-400" />
                <StatCard label="Avg Underwriter Delay" value={formatDuration(summary.avg_uw_delay_seconds)} sub="Time in pending review" color="text-violet-400" />
              </div>
            </div>
            </div>

            {/* Response Delay Line Chart */}
            {chartData.length > 0 && (
              <div className="bg-[#1e2030] rounded-xl border border-white/5 p-6">
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 rounded-full bg-teal-400" />
                  <h3 className="text-sm font-semibold text-white">Automated Processing Delay</h3>
                </div>
                <p className="text-xs text-white/30 mb-4 ml-4">Time from email ingestion to quote completion (seconds)</p>
                <SvgLineChart data={chartData} />
              </div>
            )}

            {/* Quotes by Category */}
            {barData.length > 0 && (
              <div className="bg-[#1e2030] rounded-xl border border-white/5 p-6">
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 rounded-full bg-purple-400" />
                  <h3 className="text-sm font-semibold text-white">Quotes by Risk Category</h3>
                </div>
                <p className="text-xs text-white/30 mb-4 ml-4">Distribution of quotes across industry categories</p>
                <SvgBarChart data={barData} />
              </div>
            )}

            {/* Category detail table */}
            {categoryData.length > 0 && (
              <div className="bg-[#1e2030] rounded-xl border border-white/5 overflow-hidden">
                <div className="px-6 py-4 border-b border-white/5">
                  <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-cyan-400" />
                    <h3 className="text-sm font-semibold text-white">Category Breakdown</h3>
                  </div>
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-white/[0.02] text-left">
                      <th className="px-6 py-2 text-[11px] text-white/30 uppercase tracking-wider font-medium">Category</th>
                      <th className="px-6 py-2 text-[11px] text-white/30 uppercase tracking-wider font-medium text-right">Quotes</th>
                      <th className="px-6 py-2 text-[11px] text-white/30 uppercase tracking-wider font-medium text-right">Avg Risk Score</th>
                      <th className="px-6 py-2 text-[11px] text-white/30 uppercase tracking-wider font-medium text-right">Avg Premium</th>
                    </tr>
                  </thead>
                  <tbody>
                    {categoryData.map((c) => (
                      <tr key={c.risk_category} className="border-t border-white/[0.03] hover:bg-white/[0.02]">
                        <td className="px-6 py-2.5">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-semibold ${RISK_CAT_COLORS[c.risk_category] || "bg-white/10 text-white/50"}`}>
                            {c.risk_category.replace(/_/g, " ")}
                          </span>
                        </td>
                        <td className="px-6 py-2.5 text-right font-medium text-white">{c.count}</td>
                        <td className="px-6 py-2.5 text-right text-white/60">{c.avg_risk_score != null ? c.avg_risk_score.toFixed(1) : "--"}</td>
                        <td className="px-6 py-2.5 text-right text-white/60">{c.avg_premium != null ? formatCurrency(c.avg_premium) : "--"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

// ─── Page: Quotes ───────────────────────────────────────────────────────────

const QUOTE_STATUS_FILTERS: { value: string; label: string; color: string }[] = [
  { value: "all", label: "All", color: "bg-white/10 text-white" },
  { value: "auto-approved", label: "Auto-Approved", color: "bg-emerald-500/20 text-emerald-400" },
  { value: "pending-review", label: "Pending Review", color: "bg-amber-500/20 text-amber-400" },
  { value: "auto-declined", label: "Auto-Declined", color: "bg-red-500/20 text-red-400" },
  { value: "uw-approved", label: "UW Approved", color: "bg-emerald-500/20 text-emerald-400" },
  { value: "uw-declined", label: "UW Declined", color: "bg-red-500/20 text-red-400" },
  { value: "uw-info", label: "Info Requested", color: "bg-blue-500/20 text-blue-400" },
  { value: "in-progress", label: "In Progress", color: "bg-cyan-500/20 text-cyan-400" },
];

function QuotesPage({ quotes, loading, error }: {
  quotes: Quote[]; loading: boolean; error: string | null;
}) {
  const [filter, setFilter] = useState("all");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [showPdf, setShowPdf] = useState(false);
  const [stepData, setStepData] = useState<Record<string, unknown> | null>(null);
  const [stepLoading, setStepLoading] = useState(false);
  const [activeStep, setActiveStep] = useState<string | null>(null);

  const filtered = filter === "all"
    ? quotes
    : filter === "in-progress"
      ? quotes.filter((q) => !q.decision_tag)
      : quotes.filter((q) => q.decision_tag === filter);

  const selected = quotes.find((q) => q.email_id === selectedId) || null;

  const loadStep = async (emailId: string, stepKey: string) => {
    if (activeStep === stepKey) { setActiveStep(null); setStepData(null); return; }
    setActiveStep(stepKey); setStepData(null); setStepLoading(true);
    try {
      const r = await fetch(`/api/quotes/${emailId}/step/${stepKey}`);
      if (r.ok) setStepData(await r.json());
    } catch { /* silent */ } finally { setStepLoading(false); }
  };

  const handleSelect = (q: Quote) => {
    setSelectedId(q.email_id);
    setShowPdf(false);
    setStepData(null);
    setActiveStep(null);
  };

  const pdfHref = selected ? pdfUrl(selected.pdf_path) : null;

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="Quotes"
        subtitle="All processed quotes and their details"
        right={<span className="text-sm text-white/40">{filtered.length} of {quotes.length} quote{quotes.length !== 1 ? "s" : ""}</span>}
      />

      {/* Filter bar */}
      <div className="px-6 pt-4 pb-2">
        <div className="flex flex-wrap gap-1.5">
          {QUOTE_STATUS_FILTERS.map((f) => {
            const count = f.value === "all"
              ? quotes.length
              : f.value === "in-progress"
                ? quotes.filter((q) => !q.decision_tag).length
                : quotes.filter((q) => q.decision_tag === f.value).length;
            const active = filter === f.value;
            return (
              <button
                key={f.value}
                onClick={() => setFilter(f.value)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                  active
                    ? f.color
                    : "bg-white/5 text-white/30 hover:bg-white/10 hover:text-white/50"
                }`}
              >
                {f.label} <span className={active ? "opacity-70" : "opacity-50"}>({count})</span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="flex flex-1 min-h-0 px-6 pb-6 gap-5">
        {/* Left: Quote list */}
        <div className="w-[380px] shrink-0 flex flex-col bg-[#1e2030] border border-white/5 rounded-xl overflow-hidden">
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-12 text-sm text-white/40">
                <div className="w-4 h-4 border-2 border-white/20 border-t-white/60 rounded-full step-spinner mr-2" />
                Loading...
              </div>
            ) : error && quotes.length === 0 ? (
              <div className="py-12 text-center text-sm text-red-400/60">{error}</div>
            ) : filtered.length === 0 ? (
              <div className="py-12 text-center text-sm text-white/40">
                {quotes.length === 0 ? "No quotes in the pipeline yet" : "No quotes match this filter"}
              </div>
            ) : (
              filtered.map((q) => (
                <button
                  key={q.email_id}
                  onClick={() => handleSelect(q)}
                  className={`w-full text-left px-4 py-3 border-b border-white/5 transition-colors ${
                    q.email_id === selectedId
                      ? "bg-brick-primary/10 border-l-2 border-l-brick-primary"
                      : "hover:bg-white/5 border-l-2 border-l-transparent"
                  }`}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <span className={`text-sm font-medium truncate flex-1 ${q.business_name ? "text-white/90" : "text-white/40 italic"}`}>
                      {q.business_name || "Processing..."}
                    </span>
                    <DecisionBadge tag={q.decision_tag} />
                    {q.total_premium != null && (
                      <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${q.adjusted_premium != null ? "bg-white/5 text-white/40 line-through" : "bg-white/10 text-white/70"}`}>{formatCurrency(q.total_premium)}</span>
                    )}
                    {q.adjusted_premium != null && (
                      <span className="px-2 py-0.5 rounded text-[11px] font-semibold bg-emerald-500/10 text-emerald-400">{formatCurrency(q.adjusted_premium)}</span>
                    )}
                  </div>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <RiskBadge band={q.risk_band} />
                    </div>
                    <span className="text-[11px] text-white/40">{relativeTime(q.ingestion_timestamp)}</span>
                  </div>
                </button>
              ))
            )}
          </div>
          <div className="px-3 py-2 border-t border-white/5 text-[11px] text-white/40 text-center">
            {filtered.length} of {quotes.length} quotes
          </div>
        </div>

        {/* Right: Detail side panel */}
        <div className="flex-1 min-w-0 overflow-y-auto">
          {!selected ? (
            <div className="flex items-center justify-center h-full bg-[#1e2030] border border-white/5 rounded-xl">
              <div className="text-center text-white/40">
                <svg className="w-12 h-12 mx-auto mb-3 text-white/20" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                </svg>
                <p className="text-sm font-medium text-white/60">Select a quote</p>
                <p className="text-xs mt-1 text-white/40">Click a quote on the left to see its details</p>
              </div>
            </div>
          ) : (
            <div className="bg-[#1e2030] border border-white/5 rounded-xl overflow-hidden expand-enter">
              {/* Header */}
              <div className="px-6 py-4 border-b border-white/5">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-white">{selected.business_name || "Processing..."}</h2>
                    <p className="text-xs text-white/40 mt-0.5">
                      {selected.sender_name && <>{selected.sender_name} &middot; {selected.sender_email}</>}
                      {!selected.sender_name && selected.file_name}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <RiskBadge band={selected.risk_band} />
                    <DecisionBadge tag={selected.decision_tag} />
                  </div>
                </div>
              </div>

              {/* Key metrics */}
              <div className="px-6 py-4 border-b border-white/5">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  {[
                    ["Quote #", selected.quote_number ?? "--"],
                    ["Total Premium", selected.adjusted_premium != null
                      ? `${formatCurrency(selected.adjusted_premium)} (was ${formatCurrency(selected.total_premium)})`
                      : formatCurrency(selected.total_premium)],
                    ["Risk Band", selected.risk_band ?? "--"],
                    ["Risk Score", selected.risk_score != null ? selected.risk_score.toFixed(1) : "--"],
                    ["Revenue", formatCurrency(selected.annual_revenue)],
                    ["Employees", selected.num_employees?.toLocaleString() ?? "--"],
                    ["Category", selected.risk_category?.replace(/_/g, " ") ?? "--"],
                    ["Coverages", selected.coverages_requested ?? "--"],
                  ].map(([label, val]) => (
                    <div key={label as string} className={`${(label as string) === "Coverages" ? "lg:col-span-2" : ""}`}>
                      <p className="text-[10px] text-white/40 uppercase tracking-wider">{label as string}</p>
                      <p className="text-sm font-medium text-white/80 mt-0.5">{val as string}</p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Review summary */}
              {selected.review_summary && (
                <div className="px-6 py-4 border-b border-white/5">
                  <p className="text-[10px] text-white/40 uppercase tracking-wider mb-1">Review Summary</p>
                  <p className="text-sm text-white/70 leading-relaxed">{selected.review_summary}</p>
                </div>
              )}

              {/* Underwriter notes */}
              {(selected.uw_notes || selected.uw_info_request) && (
                <div className="px-6 py-4 border-b border-white/5 bg-[#13141f]/50">
                  <h4 className="text-[10px] text-white/40 uppercase tracking-wider mb-2">Underwriter Decision</h4>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    {selected.uw_notes && (
                      <div className="sm:col-span-2">
                        <p className="text-[10px] text-white/40 uppercase tracking-wider">Notes</p>
                        <p className="text-sm text-white/70 mt-0.5 leading-relaxed">{selected.uw_notes}</p>
                      </div>
                    )}
                    {selected.uw_info_request && (
                      <div className="sm:col-span-2">
                        <p className="text-[10px] text-white/40 uppercase tracking-wider">Information Requested</p>
                        <p className="text-sm text-white/70 mt-0.5 leading-relaxed">{selected.uw_info_request}</p>
                      </div>
                    )}
                    {selected.uw_surcharge_pct != null && selected.uw_surcharge_pct > 0 && (
                      <div>
                        <p className="text-[10px] text-white/40 uppercase tracking-wider">Surcharge</p>
                        <p className="text-sm font-medium text-white/80 mt-0.5">{selected.uw_surcharge_pct}%</p>
                      </div>
                    )}
                    {selected.uw_discount_pct != null && selected.uw_discount_pct > 0 && (
                      <div>
                        <p className="text-[10px] text-white/40 uppercase tracking-wider">Discount</p>
                        <p className="text-sm font-medium text-white/80 mt-0.5">{selected.uw_discount_pct}%</p>
                      </div>
                    )}
                    {selected.uw_decided_at && (
                      <div>
                        <p className="text-[10px] text-white/40 uppercase tracking-wider">Decided At</p>
                        <p className="text-sm text-white/60 mt-0.5">{new Date(selected.uw_decided_at).toLocaleString()}</p>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Status / timestamps */}
              <div className="px-6 py-4 border-b border-white/5">
                <div className="flex flex-wrap gap-4">
                  <div>
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Status</p>
                    <p className="text-sm font-medium text-white/80 mt-0.5">{selected.final_status ?? "In Progress"}</p>
                  </div>
                  <div>
                    <p className="text-[10px] text-white/40 uppercase tracking-wider">Received</p>
                    <p className="text-sm text-white/60 mt-0.5">{new Date(selected.ingestion_timestamp).toLocaleString()}</p>
                  </div>
                  {selected.pdf_status && (
                    <div>
                      <p className="text-[10px] text-white/40 uppercase tracking-wider">PDF Status</p>
                      <p className="text-sm text-white/60 mt-0.5">{selected.pdf_status}</p>
                    </div>
                  )}
                </div>
              </div>

              {/* Pipeline progress */}
              <div className="px-6 py-4 border-b border-white/5">
                <p className="text-[10px] text-white/40 uppercase tracking-wider mb-2">Pipeline Progress</p>
                <MiniPipeline steps={selected.steps} decisionTag={selected.decision_tag} />
                <div className="flex flex-wrap gap-1.5 mt-3">
                  {STEP_KEYS.map((key, i) => {
                    const done = selected.steps[key];
                    return (
                      <button
                        key={key}
                        disabled={!done}
                        onClick={() => loadStep(selected.email_id, key)}
                        className={`px-2 py-1 rounded text-[10px] font-medium transition-colors ${
                          activeStep === key
                            ? "bg-brick-primary text-white"
                            : done
                              ? "bg-white/10 text-white/60 hover:bg-white/15 hover:text-white/80 cursor-pointer"
                              : "bg-white/5 text-white/20 cursor-default"
                        }`}
                      >
                        {STEP_LABELS[i]}
                      </button>
                    );
                  })}
                </div>
                {activeStep && (
                  <div className="mt-3">
                    {stepLoading ? (
                      <div className="flex items-center gap-2 py-4 justify-center text-white/40">
                        <div className="w-4 h-4 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
                        <span className="text-sm">Loading...</span>
                      </div>
                    ) : stepData ? (
                      <StepDetailPanel
                        data={stepData}
                        stepLabel={STEP_LABELS[STEP_KEYS.indexOf(activeStep as keyof QuoteSteps)]}
                        pdfPath={activeStep === "completed" || activeStep === "pdf_created" ? selected.pdf_path : null}
                      />
                    ) : (
                      <div className="py-4 text-center text-sm text-white/40">No data available</div>
                    )}
                  </div>
                )}
              </div>

              {/* View PDF button */}
              {pdfHref && (
                <div className="px-6 py-4">
                  <button
                    onClick={() => setShowPdf(true)}
                    className="inline-flex items-center gap-2 px-5 py-2.5 bg-brick-primary hover:bg-brick-primary/90 text-white text-sm font-medium rounded-lg transition-colors shadow-sm"
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                    </svg>
                    View PDF Quote
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* PDF Modal */}
      {showPdf && pdfHref && <PdfModal pdfPath={pdfHref} onClose={() => setShowPdf(false)} />}
    </div>
  );
}

// ─── Placeholder page ───────────────────────────────────────────────────────

function PlaceholderPage({ title, description }: { title: string; description: string }) {
  return (
    <div>
      <PageHeader title={title} subtitle={description} />
      <div className="flex items-center justify-center h-[50vh]">
        <div className="text-center">
          <div className="w-12 h-12 rounded-full bg-white/5 flex items-center justify-center mx-auto mb-4">
            <svg className="w-6 h-6 text-white/20" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p className="text-sm text-white/40">Coming soon</p>
        </div>
      </div>
    </div>
  );
}

// ─── App ────────────────────────────────────────────────────────────────────

export default function App() {
  const [page, setPage] = useState<Page>("analytics");
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [darkMode, setDarkMode] = useState(() => {
    const saved = localStorage.getItem("mail2quote-theme");
    return saved ? saved === "dark" : true;
  });

  const toggleTheme = useCallback(() => {
    setDarkMode((prev) => {
      const next = !prev;
      localStorage.setItem("mail2quote-theme", next ? "dark" : "light");
      return next;
    });
  }, []);

  const fetchQuotes = useCallback(async () => {
    try {
      const r = await fetch("/api/quotes");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setQuotes(d.quotes || []);
      setError(null);
    } catch (e: unknown) {
      if (loading) setError(e instanceof Error ? e.message : "Failed");
    } finally { setLoading(false); }
  }, [loading]);

  useEffect(() => {
    fetchQuotes();
    const t = setInterval(fetchQuotes, 3000);
    return () => clearInterval(t);
  }, [fetchQuotes]);

  return (
    <div className={`flex h-screen bg-[#13141f] overflow-hidden ${darkMode ? "" : "light-mode"}`}>
      <Sidebar activePage={page} onNavigate={setPage} darkMode={darkMode} onToggleTheme={toggleTheme} />
      <main className="flex-1 min-w-0 ml-56 h-full overflow-y-auto flex flex-col">
        {page === "email_intake" && <EmailIntakePage />}
        {page === "quotes" && <QuotesPage quotes={quotes} loading={loading} error={error} />}
        {page === "processing" && <QuoteProcessingPage quotes={quotes} loading={loading} error={error} />}
        {page === "placeholder1" && <UnderwriterReviewPage />}
        {page === "analytics" && <AnalyticsPage />}
      </main>
    </div>
  );
}
