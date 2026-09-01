export type HealthStatus = {
  status: string;
  kafka_connected: boolean;
  details?: string | null;
};

export type EventRecord = {
  event_id?: string | null;
  event_type?: string | null;
  event_timestamp?: string | null;
  customer_id?: string | null;
  session_id?: string | null;
  product_id?: string | null;
  quantity?: number | null;
  unit_price?: number | null;
  subtotal?: number | null;
  discount_amount?: number | null;
  shipping_amount?: number | null;
  tax_amount?: number | null;
  total_amount?: number | null;
  currency?: string | null;
  payment_method?: string | null;
  [key: string]: unknown;
};

export type Statistics = {
  total_events: number;
  valid_events: number;
  malformed_events: number;
  consumer_errors: number;
  events_in_memory: number;
};
