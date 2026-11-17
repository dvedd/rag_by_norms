export interface Source {
  id: string;
  filename: string;
  text: string;
  section_path: string;
  headings: string[];
  is_table: boolean;
  score: number;
}

export interface SearchResponse {
  answer: string | null;
  results: Source[];
  effective_query: string;
  was_rewritten: boolean;
  answer_mode?: string;
  latency_ms?: number;
  query_id?: string;
  client_state?: "done" | "stopped" | "error";
}

export type Stage = "accepted" | "rewriting" | "searching" | "preparing" | "generating";
export type StreamEvent =
  | { event: "status"; data: { stage: Stage; query_id?: string } }
  | { event: "sources"; data: { items: Source[]; effective_query?: string } }
  | { event: "token"; data: { text: string } }
  | { event: "done"; data: SearchResponse }
  | { event: "error"; data: { message: string; query_id?: string } };

export interface SearchEntry {
  id: string;
  remoteId?: string;
  query: string;
  created_at: string;
  response: SearchResponse;
  state: "streaming" | "done" | "stopped" | "error";
  stage: Stage;
  error?: string;
}

export interface SearchOptions {
  filename_filter: string;
  only_tables: boolean;
  use_rewriter: boolean;
}

export interface User {
  id: string;
  display_name: string;
  email: string;
}

export interface Account {
  token: string;
  session_id: string;
  expires_at: string;
  user: User;
}
