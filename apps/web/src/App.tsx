import { WorkspaceColumn } from "./components/WorkspaceColumn";

const notebookItems = [
  "Investigation created",
  "Structured plan proposed",
  "Hypotheses generated",
  "Datasets discovered",
  "Merge plan awaiting approval",
];

const workspaceCards = [
  {
    title: "Research Plan",
    body: "Turn an open-ended market question into a typed, inspectable research plan with explicit constraints and scope.",
  },
  {
    title: "Hypothesis Cards",
    body: "Present competing theses with mechanisms, falsifiers, expected drivers, and user steering controls.",
  },
  {
    title: "Analysis Workspace",
    body: "Run correlation, regression, event study, backtest, and regime analysis on reproducible datasets.",
  },
];

const inspectorItems = [
  "Prompt version and model",
  "Source provenance",
  "Lag assumptions",
  "Warnings and caveats",
  "Exact transformations",
];

export default function App() {
  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Eureka</p>
          <h1>Workflow-first quant research workspace</h1>
        </div>
        <div className="topbar-meta">
          <span>GPT-5.4</span>
          <span>Responses API</span>
          <span>Notebook provenance</span>
        </div>
      </header>
      <main className="workspace-grid">
        <WorkspaceColumn
          title="Notebook"
          subtitle="Timeline and branch rail"
          items={notebookItems}
          tone="rail"
        />
        <section className="workspace-center">
          <div className="hero-card">
            <p className="eyebrow">Seed Investigation</p>
            <h2>Cooling inflation, falling real yields, and semiconductor rotation</h2>
            <p>
              Eureka is structured around inspectable stages, user approvals, and exact provenance.
              This scaffold provides the shell for plan review, merge approval, and analysis execution.
            </p>
          </div>
          <div className="card-grid">
            {workspaceCards.map((card) => (
              <article className="content-card" key={card.title}>
                <h3>{card.title}</h3>
                <p>{card.body}</p>
              </article>
            ))}
          </div>
        </section>
        <WorkspaceColumn
          title="Inspector"
          subtitle="Warnings, provenance, and assumptions"
          items={inspectorItems}
          tone="inspector"
        />
      </main>
    </div>
  );
}
