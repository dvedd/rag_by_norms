import { Layers3 } from "lucide-react";

export function Brand() {
  return <div className="brand">
    <span className="brand-symbol"><Layers3 size={22} strokeWidth={1.7} /></span>
    <span>Engineering<span className="brand-suffix">RAG</span><small>Основание для решений</small></span>
  </div>;
}
