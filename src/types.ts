export interface ColumnDefinition {
  name: string;
  type: string;
}

export interface Table {
  id: number;
  name: string;
  schema_id: number;
  database_id: number;
  columns: ColumnDefinition[];
  mps: Micropartition[];
}

export interface Micropartition {
  id: number;
  table_id: number;
  rows: number;
  filesize: number;
  stats: MicropartitionStats;
}

export interface ColumnStatistics {
  index: number;
  name: string;
  min: any;  // The type can vary based on the column type
  max: any;  // The type can vary based on the column type
  null_count: number;
}

export interface MicropartitionStats {
  id: number;
  rows: number;
  filesize: number;
  columns: ColumnStatistics[];
} 