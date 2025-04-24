{% snapshot snapshot_test_rsi %}
    {{
        config(
            target_schema = 'snapshot',
            unique_key    = ['symbol', 'date'],
            strategy      = 'check',
            check_cols    = ['avg_gain', 'avg_loss', 'rsi_14'],
            invalidate_hard_deletes = True
        )
    }}

    SELECT * FROM {{ ref('test_rsi') }}

{% endsnapshot %}