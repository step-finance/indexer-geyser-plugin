use indexer_rabbitmq::geyser::AccountUpdate;
use solana_program::pubkey::Pubkey;

use solana_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;
pub fn create_account_update(
    account: &ReplicaAccountInfoVersions,
    slot: u64,
    is_startup: bool,
) -> AccountUpdate {
    match account {
        ReplicaAccountInfoVersions::V0_0_1(account) => AccountUpdate {
            key: Pubkey::new_from_array(account.pubkey.try_into().unwrap()),
            lamports: account.lamports,
            slot,
            data: account.data.to_owned(),
            executable: account.executable,
            owner: Pubkey::new_from_array(account.owner.try_into().unwrap()),
            rent_epoch: account.rent_epoch,
            is_startup,
            txn_signature: None,
            write_version: account.write_version,
        },
        ReplicaAccountInfoVersions::V0_0_2(account) => AccountUpdate {
            key: Pubkey::new_from_array(account.pubkey.try_into().unwrap()),
            lamports: account.lamports,
            slot,
            data: account.data.to_owned(),
            executable: account.executable,
            owner: Pubkey::new_from_array(account.owner.try_into().unwrap()),
            rent_epoch: account.rent_epoch,
            is_startup,
            txn_signature: account.txn_signature.map(std::string::ToString::to_string),
            write_version: account.write_version,
        },
    }
}
