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
  risk_score: number | null;
  risk_band: string | null;
  review_summary: string | null;
  total_premium: number | null;
  quote_number: string | null;
  pdf_path: string | null;
  pdf_status: string | null;
  final_status: string | null;
  steps: QuoteSteps;
}

type Page = "processing" | "placeholder1" | "placeholder2";

// ─── Constants ──────────────────────────────────────────────────────────────

const STEP_KEYS: (keyof QuoteSteps)[] = [
  "received", "parsed", "enriched", "features", "risk_scoring",
  "quote_review", "quote_creation", "pdf_created", "completed",
];

const STEP_LABELS: string[] = [
  "Email Received", "LLM Parsing", "Data Enrichment", "Featurization",
  "Risk Scoring", "Quote Review", "Quote Creation", "PDF Creation", "Completed",
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
    Low: "bg-emerald-500/10 text-emerald-600", Medium: "bg-amber-500/10 text-amber-600",
    High: "bg-orange-500/10 text-orange-600", "Very High": "bg-red-500/10 text-red-600",
  };
  return <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${c[band] || "bg-gray-100 text-gray-500"}`}>{band}</span>;
}

function DecisionBadge({ tag }: { tag: string | null }) {
  if (!tag) return null;
  const c: Record<string, string> = {
    "auto-approved": "bg-emerald-500/10 text-emerald-600",
    "pending-review": "bg-amber-500/10 text-amber-600",
    "auto-declined": "bg-red-500/10 text-red-600",
  };
  return <span className={`px-2 py-0.5 rounded text-[11px] font-semibold ${c[tag] || "bg-gray-100 text-gray-500"}`}>{tag}</span>;
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
            {i > 0 && <div className={`w-1.5 h-px ${done ? "bg-emerald-400" : warn ? "bg-amber-300" : "bg-gray-300"}`} />}
            <div className={`w-2.5 h-2.5 rounded-full ${
              done ? "bg-emerald-500" : warn ? "bg-amber-400" : spin ? "border border-brick-primary border-t-transparent step-spinner" : "bg-gray-300"
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
        className="bg-white rounded-xl shadow-2xl flex flex-col overflow-hidden"
        style={{ width: "min(92vw, 1100px)", height: "85vh" }}
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 bg-sidebar shrink-0">
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
        <div className="flex justify-end px-5 py-3 border-t border-gray-200 bg-gray-50 shrink-0">
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
    <div className="expand-enter bg-white border border-gray-200 rounded-lg p-5 mt-4">
      <div className="flex items-center justify-between mb-4">
        <h4 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">{stepLabel}</h4>
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
            <div key={key} className={`flex flex-col py-2 border-b border-gray-50 ${long ? "sm:col-span-2 lg:col-span-3" : ""}`}>
              <span className="text-[10px] text-gray-400 uppercase tracking-wider">{label}</span>
              <span className={`text-[13px] text-gray-700 ${long ? "leading-relaxed mt-0.5" : "font-medium"}`}>{fmtVal(key, val)}</span>
            </div>
          );
        })}
      </div>
      {timestamps.length > 0 && (
        <div className="mt-4 pt-3 border-t border-gray-100 flex flex-wrap gap-6">
          {timestamps.map(([key, val]) => (
            <div key={key} className="text-[11px] text-gray-400">
              <span className="uppercase tracking-wide">{COL_LABELS[key] || key}: </span>
              <span className="text-gray-500">{fmtVal(key, val)}</span>
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
                    active ? "bg-brick-dark ring-2 ring-brick-primary ring-offset-2"
                    : done ? "bg-emerald-500"
                    : warn ? "bg-amber-400"
                    : spin ? "border-[3px] border-brick-primary border-t-transparent step-spinner bg-white"
                    : "bg-gray-200"
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
                  {!done && !spin && !warn && <span className="text-[11px] text-gray-400 font-medium">{i + 1}</span>}
                </button>
                <span className={`text-[10px] mt-1.5 text-center leading-tight font-medium ${
                  active ? "text-brick-dark" : done ? "text-emerald-600" : warn ? "text-amber-600" : spin ? "text-brick-primary" : "text-gray-400"
                }`}>
                  {warn ? "Pending Review" : STEP_LABELS[i]}
                </span>
              </div>
              {i < STEP_KEYS.length - 1 && (
                <div className={`flex-1 h-px mx-0.5 -mt-5 ${flags[i + 1] ? "bg-emerald-400" : done ? "bg-emerald-200" : warn ? "bg-amber-200" : "bg-gray-200"}`} />
              )}
            </div>
          );
        })}
      </div>

      {sel !== null && (
        <div className="pb-1">
          {loading ? (
            <div className="flex items-center gap-2 py-8 justify-center text-gray-400">
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
            <div className="py-6 text-center text-sm text-gray-400">No data available</div>
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
    <div className={`bg-white rounded-lg border transition-all duration-150 ${
      isExpanded ? "border-gray-300 shadow-sm" : "border-gray-200 hover:border-gray-300"
    }`}>
      <div className="px-5 py-3.5 cursor-pointer select-none" onClick={onToggle}>
        <div className="flex items-center gap-5">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-gray-900 truncate">{quote.business_name}</h3>
              <RiskBadge band={quote.risk_band} />
              <DecisionBadge tag={quote.decision_tag} />
            </div>
            <p className="text-[12px] text-gray-400 mt-0.5 truncate">{quote.sender_name} &middot; {quote.sender_email}</p>
          </div>
          <div className="text-right hidden sm:block min-w-[90px]">
            <p className="text-sm font-semibold text-gray-900">{formatCurrency(quote.total_premium)}</p>
            <p className="text-[11px] text-gray-400">premium</p>
          </div>
          <div className="hidden md:block"><MiniPipeline steps={quote.steps} decisionTag={quote.decision_tag} /></div>
          <p className="text-[11px] text-gray-400 min-w-[60px] text-right">{relativeTime(quote.ingestion_timestamp)}</p>
          <svg className={`w-4 h-4 text-gray-400 transition-transform duration-150 shrink-0 ${isExpanded ? "rotate-180" : ""}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </div>
      {isExpanded && (
        <div className="expand-enter border-t border-gray-100 px-5 pt-3 pb-4">
          <p className="text-[11px] text-gray-400 mb-1">Click a completed step to inspect its data</p>
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
    key: "placeholder2",
    label: "Analytics",
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
      </svg>
    ),
  },
];

function Sidebar({ activePage, onNavigate }: { activePage: Page; onNavigate: (p: Page) => void }) {
  return (
    <aside className="w-56 bg-sidebar shrink-0 flex flex-col border-r border-white/5 fixed top-0 left-0 h-full z-40">
      {/* Logo area */}
      <div className="px-4 h-14 flex items-center gap-3 border-b border-white/5">
        <img
          src="https://companieslogo.com/img/orig/databricks.D-0e162e58.png?t=1720244494"
          alt="Logo"
          className="h-7 w-7 object-contain"
        />
        <span className="text-white/90 font-semibold text-sm tracking-tight">BricksHouse</span>
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

      {/* Status */}
      <div className="px-4 py-3 border-t border-white/5">
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

function QuoteProcessingPage({ quotes, loading, error }: {
  quotes: Quote[]; loading: boolean; error: string | null;
}) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  return (
    <div>
      <PageHeader
        title="Quote Processing"
        subtitle="Real-time pipeline status for incoming quote requests"
        right={<span className="text-sm text-white/40">{quotes.length} quote{quotes.length !== 1 ? "s" : ""}</span>}
      />
      <div className="p-6 max-w-[1200px] mx-auto">

      {loading && (
        <div className="flex items-center justify-center py-20">
          <div className="w-6 h-6 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
          <span className="ml-3 text-gray-400 text-sm">Loading...</span>
        </div>
      )}
      {!loading && error && quotes.length === 0 && (
        <div className="bg-red-50 border border-red-100 rounded-lg p-6 text-center">
          <p className="text-red-600 text-sm font-medium">Failed to connect</p>
          <p className="text-red-400 text-xs mt-1">{error}</p>
        </div>
      )}
      {!loading && !error && quotes.length === 0 && (
        <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
          <p className="text-gray-400 text-sm">No quotes in the pipeline yet</p>
        </div>
      )}

      <div className="space-y-2">
        {quotes.map((q) => (
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
            <div className="bg-white rounded-lg border border-gray-200 p-8 text-center">
              <p className="text-sm text-gray-400">No quotes pending review</p>
            </div>
          )}
          {queue.map((q) => (
            <button
              key={q.email_id}
              onClick={() => handleSelect(q)}
              className={`w-full text-left px-4 py-3 rounded-lg border transition-colors ${
                selected?.email_id === q.email_id
                  ? "border-brick-primary bg-brick-primary/5"
                  : "border-gray-200 bg-white hover:border-gray-300"
              }`}
            >
              <div className="flex items-center justify-between">
                <span className="text-sm font-semibold text-gray-900 truncate">{q.business_name}</span>
                <RiskBadge band={q.risk_band} />
              </div>
              <p className="text-[11px] text-gray-400 mt-0.5 truncate">{q.sender_name} &middot; {q.sender_email}</p>
              <div className="flex items-center justify-between mt-1.5">
                <span className="text-[11px] text-gray-400">{relativeTime(q.ingestion_timestamp)}</span>
                <span className="text-[11px] font-medium text-gray-500">Score: {q.risk_score?.toFixed(0)}</span>
              </div>
            </button>
          ))}
        </div>

        {/* Right: Detail panel */}
        <div className="flex-1 min-w-0">
          {!selected ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-gray-400">Select a quote to review</p>
            </div>
          ) : submitted ? (
            <div className="bg-white rounded-lg border border-gray-200 p-8 text-center expand-enter">
              <div className={`w-14 h-14 rounded-full flex items-center justify-center mx-auto mb-4 ${
                submitted === "uw-approved" ? "bg-emerald-100" : submitted === "uw-declined" ? "bg-red-100" : "bg-amber-100"
              }`}>
                {submitted === "uw-approved" && (
                  <svg className="w-7 h-7 text-emerald-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                  </svg>
                )}
                {submitted === "uw-declined" && (
                  <svg className="w-7 h-7 text-red-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                )}
                {submitted === "uw-info" && (
                  <svg className="w-7 h-7 text-amber-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21.75 6.75v10.5a2.25 2.25 0 01-2.25 2.25h-15a2.25 2.25 0 01-2.25-2.25V6.75m19.5 0A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25m19.5 0v.243a2.25 2.25 0 01-1.07 1.916l-7.5 4.615a2.25 2.25 0 01-2.36 0L3.32 8.91a2.25 2.25 0 01-1.07-1.916V6.75" />
                  </svg>
                )}
              </div>
              <h3 className="text-lg font-semibold text-gray-900">
                {submitted === "uw-approved" ? "Quote Approved" : submitted === "uw-declined" ? "Quote Declined" : "Information Requested"}
              </h3>
              <p className="text-sm text-gray-400 mt-1">{selected.business_name}</p>
              <button onClick={() => { setSelected(null); setSubmitted(null); }}
                className="mt-4 text-sm text-brick-primary hover:underline">Back to queue</button>
            </div>
          ) : (
            <div className="bg-white rounded-lg border border-gray-200 overflow-hidden expand-enter">
              {/* Quote header */}
              <div className="px-6 py-4 border-b border-gray-100">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-gray-900">{selected.business_name}</h2>
                    <p className="text-xs text-gray-400 mt-0.5">
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
              <div className="px-6 py-4 border-b border-gray-100 bg-amber-50/50">
                <h4 className="text-xs font-semibold text-amber-700 uppercase tracking-wider mb-2">Why This Requires Review</h4>
                <p className="text-sm text-gray-700 leading-relaxed">{selected.review_summary || "No review summary available."}</p>
              </div>

              {/* Details grid */}
              <div className="px-6 py-4 border-b border-gray-100">
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
                      <p className="text-[10px] text-gray-400 uppercase tracking-wider">{label as string}</p>
                      <p className="text-sm font-medium text-gray-800 mt-0.5">{val as string}</p>
                    </div>
                  ))}
                </div>
                {selected.worst_claim_description && (
                  <div className="mt-3 pt-3 border-t border-gray-100">
                    <p className="text-[10px] text-gray-400 uppercase tracking-wider">Worst Claim</p>
                    <p className="text-sm text-gray-700 mt-0.5">{selected.worst_claim_description}</p>
                  </div>
                )}
                {selected.special_requirements && (
                  <div className="mt-2">
                    <p className="text-[10px] text-gray-400 uppercase tracking-wider">Special Requirements</p>
                    <p className="text-sm text-gray-700 mt-0.5">{selected.special_requirements}</p>
                  </div>
                )}
              </div>

              {/* Decision section */}
              <div className="px-6 py-5">
                <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Underwriter Decision</h4>

                {/* Action buttons */}
                <div className="flex gap-2 mb-4">
                  <button onClick={() => { setAction("approve"); setDiscount(""); setSurcharge(""); }}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "approve"
                        ? "bg-emerald-500 text-white border-emerald-500"
                        : "bg-white text-gray-700 border-gray-200 hover:border-emerald-300 hover:text-emerald-700"
                    }`}>Approve</button>
                  <button onClick={() => setAction("decline")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "decline"
                        ? "bg-red-500 text-white border-red-500"
                        : "bg-white text-gray-700 border-gray-200 hover:border-red-300 hover:text-red-700"
                    }`}>Decline</button>
                  <button onClick={() => setAction("info")}
                    className={`flex-1 py-2.5 rounded-lg text-sm font-medium transition-colors border ${
                      action === "info"
                        ? "bg-amber-500 text-white border-amber-500"
                        : "bg-white text-gray-700 border-gray-200 hover:border-amber-300 hover:text-amber-700"
                    }`}>Request Info</button>
                </div>

                {/* Approve options */}
                {action === "approve" && (
                  <div className="expand-enter space-y-3 mb-4">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="text-[11px] text-gray-500 uppercase tracking-wider">Surcharge %</label>
                        <input type="number" min="0" max="100" step="0.5" value={surcharge} onChange={e => setSurcharge(e.target.value)}
                          placeholder="0" className="mt-1 w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary" />
                      </div>
                      <div>
                        <label className="text-[11px] text-gray-500 uppercase tracking-wider">Discount %</label>
                        <input type="number" min="0" max="100" step="0.5" value={discount} onChange={e => setDiscount(e.target.value)}
                          placeholder="0" className="mt-1 w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary" />
                      </div>
                    </div>
                  </div>
                )}

                {/* Request info */}
                {action === "info" && (
                  <div className="expand-enter mb-4">
                    <label className="text-[11px] text-gray-500 uppercase tracking-wider">Information Required</label>
                    <textarea value={infoRequest} onChange={e => setInfoRequest(e.target.value)}
                      rows={3} placeholder="Describe the additional information needed from the applicant..."
                      className="mt-1 w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary resize-none" />
                  </div>
                )}

                {/* Notes (always visible when action selected) */}
                {action && (
                  <div className="expand-enter mb-4">
                    <label className="text-[11px] text-gray-500 uppercase tracking-wider">Underwriter Notes</label>
                    <textarea value={notes} onChange={e => setNotes(e.target.value)}
                      rows={2} placeholder="Add notes about your decision..."
                      className="mt-1 w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary resize-none" />
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
            <div className="fixed bottom-20 right-8 w-[420px] max-h-[520px] bg-white rounded-xl shadow-2xl border border-gray-200 flex flex-col z-50 expand-enter">
              {/* Header */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100 bg-sidebar rounded-t-xl">
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
                    <div className="w-10 h-10 rounded-full bg-gray-50 flex items-center justify-center mx-auto mb-3">
                      <svg className="w-5 h-5 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                      </svg>
                    </div>
                    <p className="text-xs text-gray-400">Ask anything about <span className="font-medium text-gray-500">{selected.business_name}</span></p>
                    <div className="mt-3 flex flex-wrap gap-1.5 justify-center">
                      {["What are the main risk factors?", "Summarize the claims history", "Is the premium reasonable?"].map(q => (
                        <button key={q} onClick={() => { setChatInput(q); }}
                          className="text-[11px] px-2.5 py-1 bg-gray-50 hover:bg-gray-100 text-gray-500 rounded-full transition-colors">{q}</button>
                      ))}
                    </div>
                  </div>
                )}
                {chatHistory.map((msg, i) => (
                  <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                    <div className={`max-w-[85%] px-3 py-2 rounded-lg text-sm leading-relaxed ${
                      msg.role === "user"
                        ? "bg-brick-primary text-white rounded-br-sm"
                        : "bg-gray-50 text-gray-700 border border-gray-100 rounded-bl-sm"
                    }`}>
                      {msg.role === "assistant" ? (
                        <div className="whitespace-pre-wrap">{msg.text}</div>
                      ) : msg.text}
                    </div>
                  </div>
                ))}
                {chatLoading && (
                  <div className="flex justify-start">
                    <div className="px-3 py-2 bg-gray-50 border border-gray-100 rounded-lg rounded-bl-sm flex items-center gap-2">
                      <div className="w-3.5 h-3.5 border-2 border-brick-primary border-t-transparent rounded-full step-spinner" />
                      <span className="text-xs text-gray-400">Thinking...</span>
                    </div>
                  </div>
                )}
              </div>

              {/* Input */}
              <div className="px-3 py-3 border-t border-gray-100">
                <div className="flex gap-2">
                  <input
                    value={chatInput}
                    onChange={e => setChatInput(e.target.value)}
                    onKeyDown={e => e.key === "Enter" && !e.shiftKey && sendChat()}
                    placeholder="Ask about this quote..."
                    disabled={chatLoading}
                    className="flex-1 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brick-primary/30 focus:border-brick-primary disabled:opacity-50"
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

// ─── Placeholder page ───────────────────────────────────────────────────────

function PlaceholderPage({ title, description }: { title: string; description: string }) {
  return (
    <div>
      <PageHeader title={title} subtitle={description} />
      <div className="flex items-center justify-center h-[50vh]">
        <div className="text-center">
          <div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mx-auto mb-4">
            <svg className="w-6 h-6 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p className="text-sm text-gray-400">Coming soon</p>
        </div>
      </div>
    </div>
  );
}

// ─── App ────────────────────────────────────────────────────────────────────

export default function App() {
  const [page, setPage] = useState<Page>("processing");
  const [quotes, setQuotes] = useState<Quote[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
    <div className="flex min-h-screen bg-gray-50">
      <Sidebar activePage={page} onNavigate={setPage} />
      <main className="flex-1 min-w-0 ml-56">
        {page === "processing" && <QuoteProcessingPage quotes={quotes} loading={loading} error={error} />}
        {page === "placeholder1" && <UnderwriterReviewPage />}
        {page === "placeholder2" && <PlaceholderPage title="Analytics" description="Coming soon -- pipeline metrics and dashboards" />}
      </main>
    </div>
  );
}
