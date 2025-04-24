{% snapshot snapshot_test_mda %}
    {{
        config(
            target_schema = 'snapshot',
            unique_key    = ['symbol', 'date'],
            strategy      = 'check',
            check_cols    = 'all',
            invalidate_hard_deletes = True
        )
    }}

    SELECT * FROM {{ ref('test_mda') }}

{% endsnapshot %}