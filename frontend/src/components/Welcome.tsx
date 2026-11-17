import { ArrowUpRight, AudioLines, BookOpen, BrickWall, LandPlot, ScanLine } from "lucide-react";

const examples = [
  { icon: BrickWall, label: "Конструкции", query: "Как располагать стыки рабочей арматуры внахлестку?" },
  { icon: AudioLines, label: "Материалы", query: "Из каких материалов можно сделать вертикальный шумозащитный экран?" },
  { icon: LandPlot, label: "Изыскания", query: "Сколько действительны результаты инженерно-геологических изысканий?" },
  { icon: ScanLine, label: "Сейсмика", query: "Какие грунты нужно динамически испытывать при изысканиях в сейсмическом районе?" },
];

export function Welcome({ onSelect }: { onSelect: (query: string) => void }) {
  return <section className="welcome">
    <div className="welcome-intro">
      <div className="welcome-copy">
        <div className="library-tag"><BookOpen size={15} /> СП, ГОСТ и СНиП</div>
        <h1>У каждого решения<br />есть основание.</h1>
        <p>Задайте инженерный вопрос. Найдем требования<br className="desktop-break" /> в нормативной базе и соберем ответ с источниками.</p>
      </div>
      <StructureDrawing />
    </div>
    <div className="examples-heading"><h2>С чего начнем?</h2><span>Выберите вопрос или задайте свой</span></div>
    <div className="examples">
      {examples.map(({ icon: Icon, label, query }) => <button className="example" key={label} onClick={() => onSelect(query)}>
        <span className="example-category"><Icon size={17} strokeWidth={1.6} />{label}<ArrowUpRight size={16} /></span>
        <span>{query}</span>
      </button>)}
    </div>
    <p className="question-tip">Точный вопрос — точнее результат. Укажите конструкцию, материал или номер документа.</p>
  </section>;
}

function StructureDrawing() {
  return <svg className="structure-drawing" viewBox="0 0 240 240" fill="none" aria-hidden="true">
    <defs><pattern id="drawing-grid" width="16" height="16" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r=".8" fill="currentColor" opacity=".18" /></pattern></defs>
    <rect width="240" height="240" fill="url(#drawing-grid)" />
    <g stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round">
      <path d="m31 168 97 49 91-52-97-48z" fill="var(--accent-soft)" />
      <path d="m31 168 0 10 97 49 91-52v-10M128 217v10" />
      <path d="M64 149V84l14 7v65m101-11V80l-14 8v65M121 177V110l14-8v68" />
      <path d="m39 71 88 44 77-43-88-45z" fill="var(--surface)" />
      <path d="M39 71v12l88 44 77-43V72M127 115v12M69 55l87 45M94 40l88 45M65 84l76-44M96 99l76-43" />
      <path d="m24 188 98 50m19-3 85-49M15 171v-99M10 75h10m-10 94h10" strokeDasharray="3 4" opacity=".5" />
      <path d="m20 192 8-8m87 57 8-8m21-4-7-7m86-31-7-7" opacity=".5" />
    </g>
  </svg>;
}
