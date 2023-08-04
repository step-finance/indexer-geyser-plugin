# Step Finance Geyser Plugin

Forked from Holaplex's plugin, this plugin publishes to RabbitMQ

## Setup

Clone, build

## Testing

- Clone the Step Finance Solana repo (use latest `step-plugin` branch). We do have changes to Solana itself to support sending data in messages to Geyser.
- Build Solana via `cargo build --release` in its root.
- Build local repo in release mode `cargo build --release`.
- Run a local rabbitmq `docker run -it --rm -h rabbitmq --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3-management` (you can also run this with `-d` instead of `-it` to run in background, but I prefer using a seperate terminal window and stopping it when done)
- Login to rabbit to make sure it is running at http://localhost:15672/ - default login will be `guest`/`guest`. You don't need to create anything, the plugin will handle that.
- Edit `crates/plugin/sample_config.json` to point to this local rabbitmq (it should be already tho)
- Run test validator from the Solana repo dir like this: `./target/release/solana-test-validator -l $PWD/../test-ledger -r --geyser-plugin-config ../indexer-geyser-plugin/crates/plugin/sample_config.json`. This assumes the solana repo is a sibling of this repo's root.

You should now see messages hitting the exchange `devnet.processed.messages` on your local rabbit.

You can make queues to subscribe to this exchange to debug things, or to go further downstream check out the `step-confirmooor` repo.

## Config

The configuration that Holaplex has was modified slightly:

- Publish is always done a "ProductionUnchecked" meaning the exchange/queue has no suffix. This is simply easier to develop with on a small team.

- Logic surrounding filtered token lists was removed, but the config was left.  The config value for `allTokenCalls` should be set to true, otherwise the behavior is completely untested and undefined.

- Routing keys are added to published messages as so (prefix described in next bullet):
  - `<prefix>.transaction`
  - `<prefix>.instruction`
  - `<prefix>.account`
  - `multi.chain.slot_status`
  - `multi.chain.block_meta`

- All config values that were lists of keys are instead maps where the key is the address and the value is a queue prefix to use.

For example,
```
  "transactions": {
    "programs": {
      "11111111111111111111111111111111": "bob",
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "alice"
    },
    "pubkeys": {}
  }
```
Publishes transactions which use:
 
 system program to `bob.transaction`
 
 token program to `alice.transaction`
 
 both programs in same tx `multi.transaction`

 - The same is applied to other types, and where collisions can happen, the `multi` prefix is used.

 This "multi" prefix approach was used because currently the plan is just to have 2 types, so each can subscribe to its own prefix, plus multi.  If there were 3 types, then `multi` might include txs that a consumer might not want. 

 This approach was taken over publishing using the program/account pubkey as the routing key as its simpler to manage the queue bindings (they won't change).  In addition, using the matched item as the routing key provides no solution to the "multi-match" problem.

 Note: If bob and alice are both interested in the same program, setting that one's prefix to "multi" in the config itself is the right approach.