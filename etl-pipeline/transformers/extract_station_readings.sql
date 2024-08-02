SELECT    
    '{{ block_output(parse=lambda data, _vars: data[0]["id"]) }}' AS station_id
