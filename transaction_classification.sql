with searchers as (
    select
        tx_to 
    from [address_classification_results_after_stage1_and_stage2]),
    trades as (
    select 
        distinct tx_hash as hash
    from dex.trades
    where blockchain = '{{chain}}'),
    interactions as (
    select 
        distinct f.tx_hash as hash
    from {{chain}}.traces as f
      inner join dex.raw_pools as s on f."to" = s.pool
    where s.blockchain = '{{chain}}'),
    categorization as (
    select
      txs.hash,
      txs.block_date,
      txs.gas_used,
      txs.index,
      txs.priority_fee_per_gas,
      txs.success,
      case
        when (searchers.tx_to is not null) then 'mev'
        else 'non-mev'
      end as cat1,
      case 
        when (trades.hash is not null) then 'trade'
        when (interactions.hash is not null) then 'interaction'
        else 'rest'
      end as cat2
    from {{chain}}.transactions as txs
    left join searchers on searchers.tx_to = txs."to"
    left join trades on trades.hash = txs.hash
    left join interactions on interactions.hash = txs.hash)
select 
  block_date,
  success,
  cat1,
  cat2,
  avg(index) as avg_index,
  count(*) as count_tx,
  sum(gas_used) as total_gas_used
from categorization
group by 1,2,3,4
order by block_date
