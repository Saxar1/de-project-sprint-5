-- cdm.dm_courier_ledger definition

-- Drop table

-- DROP TABLE cdm.dm_courier_ledger;

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar(20) NOT NULL,
	courier_name varchar(30) NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_corier_ledger_courier_id_unique UNIQUE (courier_id),
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (((rate_avg >= (1)::numeric) AND (rate_avg <= (5)::numeric))),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month < 12))),
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2022) AND (settlement_year < 2500)))
);


-- cdm.dm_settlement_report definition

-- Drop table

-- DROP TABLE cdm.dm_settlement_report;

CREATE TABLE cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar(20) NOT NULL,
	restaurant_name varchar(20) NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_order_processing_fee_check null,
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check null,
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check null,
	CONSTRAINT dm_settlement_report_orders_count_check null,
	CONSTRAINT dm_settlement_report_orders_total_sum_check null,
	CONSTRAINT dm_settlement_report_pkey null,
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check null,
	CONSTRAINT dm_settlement_report_settlement_date_check null,
	CONSTRAINT dm_settlement_report_unique null
);


-- cdm.srv_wf_settings definition

-- Drop table

-- DROP TABLE cdm.srv_wf_settings;

CREATE TABLE cdm.srv_wf_settings (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey null,
	CONSTRAINT srv_wf_settings_workflow_key_key null
);