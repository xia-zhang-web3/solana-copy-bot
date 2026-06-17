import logoUrl from "../assets/grindscout-logo.png";

export function Brand() {
  return (
    <div className="brand">
      <div className="brand-mark" aria-hidden="true">
        <img alt="" src={logoUrl} />
      </div>
      <div>
        <div className="brand-name">GrindScout</div>
      </div>
    </div>
  );
}
