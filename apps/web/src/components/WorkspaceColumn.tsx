type WorkspaceColumnProps = {
  title: string;
  subtitle: string;
  items: string[];
  tone: "rail" | "inspector";
};

export function WorkspaceColumn({
  title,
  subtitle,
  items,
  tone,
}: WorkspaceColumnProps) {
  return (
    <aside className={`workspace-column workspace-column-${tone}`}>
      <div className="column-header">
        <p className="eyebrow">{subtitle}</p>
        <h2>{title}</h2>
      </div>
      <div className="column-list">
        {items.map((item) => (
          <div className="column-item" key={item}>
            {item}
          </div>
        ))}
      </div>
    </aside>
  );
}
