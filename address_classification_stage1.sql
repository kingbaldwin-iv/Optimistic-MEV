with
  aggs as (
    select
      *
    from (
        (
          select distinct
            project_contract_address as tx_to
          from dex_aggregator.trades
          where blockchain = '{{chain}}'
          AND project_contract_address IS NOT NULL
        )
        union
        (
          select distinct
            address as tx_to
          from dex.addresses
          where blockchain = '{{chain}}'
        )
      )
  ),
  mevs as (
    select
    tx_hash
    , tx_to
    , map_filter(
        map_zip_with(
          multimap_agg(token_sold_address, - token_sold_amount),
          multimap_agg(token_bought_address, token_bought_amount),
          (k, v1, v2) -> reduce(coalesce(v1, array[]), 0, (s, x) -> s + coalesce(x, 0), s -> s) + reduce(coalesce(v2, array[]), 0, (s, x) -> s + coalesce(x, 0), s -> s)
        ),
        (k, v) -> v <> 0
      ) as balance_changes
    from dex.trades
    where blockchain = '{{chain}}'
   and tx_to not in (
        select
          tx_to
        from aggs
      )
    group by
      1,
      2
    having
      regexp_like(
        ARRAY_JOIN(
          ARRAY_AGG(
            CONCAT(to_hex(token_sold_address), ',', to_hex(token_bought_address))
            order by
              evt_index
          ),
          ','
        ),
        '^([^,]+),(?:(?:([^,]+),\2,))+\1$'
      )
  ),
addies as 
(select
  distinct tx_to
from mevs
where all_match(map_values(balance_changes),v -> v>0) and cardinality(balance_changes) > 0) 

select tx_to from addies
  
  
