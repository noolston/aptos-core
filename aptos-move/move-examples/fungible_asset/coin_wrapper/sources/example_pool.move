module coin_wrapper::example_pool {
    use aptos_framework::aptos_account;
    use aptos_framework::fungible_asset;
    use aptos_framework::coin::{Self, Coin};
    use aptos_framework::object::{Self, ExtendRef, Object};
    use aptos_std::smart_table::{Self, SmartTable};

    use coin_wrapper::coin_wrapper;

    use std::signer;

    /// Amount has to be greater than 0.
    const EINVALID_AMOUNT: u64 = 1;

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    /// A pool that allows users to deposit coins into it while storing them as fungible assets.
    /// This keeps the code cleaner if the rest of the project is using pure fungible assets.
    struct Pool has key {
        // Balances of depositors. You can imagine that this would be used to determine sharesif the pool gains
        // interests.
        coin_balances: SmartTable<address, u64>,
        // Used to generate signer for withdraw fungible assets from the pool when users request to withdraw.
        extend_ref: ExtendRef,
    }

    /// Create a new pool that can store the coins of the given type.
    public fun create_pool<CoinType>(creator: &signer): Object<Pool> {
        let pool_constructor_ref = &object::create_object_from_account(creator);
        // We need to make the pool a fungible store to be able to store the wrapper fungible assets.
        let wrapper = coin_wrapper::create_fungible_asset<CoinType>();
        fungible_asset::create_store(pool_constructor_ref, wrapper);
        move_to(&object::generate_signer(pool_constructor_ref), Pool {
            coin_balances: smart_table::new(),
            extend_ref: object::generate_extend_ref(pool_constructor_ref),
        });

        let pool = object::object_from_constructor_ref(pool_constructor_ref);
        // Make the pool object's its own owner so we can use its signer (via ExtendRef) to withdraw the wrapper
        // fungible assets later. An alternative here is to have the pool object owned by the protocol's resource
        // account or object if one is available.
        object::transfer(creator, pool, object::object_address(&pool));

        pool
    }

    /// Deposit an amount of coins into a pool.
    public entry fun deposit_entry<CoinType>(user: &signer, pool: Object<Pool>, amount: u64) acquires Pool {
        // Withdraw the coins from the user's account, wrap them into fungible asset, and deposit them into the pool.
        let coins = coin::withdraw<CoinType>(user, amount);
        deposit(pool, coins, signer::address_of(user));
    }

    /// Non-entry version for composability with other modules on chain.
    public fun deposit<CoinType>(pool: Object<Pool>, coins: Coin<CoinType>, recipient: address) acquires Pool {
        let amount = coin::value(&coins);
        assert!(amount > 0, EINVALID_AMOUNT);
        // Wrap coins into fungible asset and deposit them into the pool.
        let wrapped_fungible_asset = coin_wrapper::wrap(coins);
        fungible_asset::deposit(pool, wrapped_fungible_asset);
        let pool_data = mut_pool(&pool);
        if (!smart_table::contains(&pool_data.coin_balances, recipient)) {
            smart_table::add(&mut pool_data.coin_balances, recipient, amount);
        } else {
            let old_amount = smart_table::borrow_mut(&mut pool_data.coin_balances, recipient);
            *old_amount = *old_amount + amount;
        };
    }

    /// Withdraw an amount of coins from a pool and deposit that into the specified recipient's account.
    public entry fun withdraw_entry<CoinType>(
        user: &signer,
        pool: Object<Pool>,
        amount: u64,
        recipient: address,
    ) acquires Pool {
        // Withdraw the fungible asset from the pool, unwrap them, and deposit the resulting coins into the user's
        // account.
        let coins = withdraw<CoinType>(user, pool, amount);
        // aptos_account::deposit_coins is used here instead of coin::deposit because it also implicitly registers
        // the receipient account to receive coins if they have not yet registered for this specific coin type.
        aptos_account::deposit_coins(recipient, coins);
    }

    /// Non-entry version for composability with other modules on chain.
    public fun withdraw<CoinType>(user: &signer, pool: Object<Pool>, amount: u64): Coin<CoinType> acquires Pool {
        let pool_data = mut_pool(&pool);
        // Update the user's balance in the pool. This errors out if they're overdrawing.
        let old_amount = smart_table::borrow_mut(&mut pool_data.coin_balances, signer::address_of(user));
        *old_amount = *old_amount - amount;
        // Withdraw the fungible asset from the pool, unwrap them, and return the resulting coins.
        let pool_signer = &object::generate_signer_for_extending(&pool_data.extend_ref);
        let fungible_assets = fungible_asset::withdraw(pool_signer, pool, amount);
        // This unwrap errors out if the wrapper fungible assets and the original CoinType don't match.
        coin_wrapper::unwrap<CoinType>(fungible_assets)
    }

    inline fun mut_pool(pool: &Object<Pool>): &mut Pool acquires Pool {
        borrow_global_mut<Pool>(object::object_address(pool))
    }

    #[test_only]
    use aptos_framework::account;
    #[test_only]
    use aptos_framework::coin::FakeMoney;

    #[test(user = @0x1, deployer = @0xcafe, recipient = @0x123)]
    fun e2e_test(user: &signer, deployer: &signer, recipient: &signer) acquires Pool {
        account::create_account_for_test(signer::address_of(user));
        account::create_account_for_test(signer::address_of(deployer));
        account::create_account_for_test(signer::address_of(recipient));
        coin_wrapper::initialize_for_test(deployer);
        coin::create_fake_money(user, user, 1000);

        let pool = create_pool<FakeMoney>(deployer);
        let user_addr = signer::address_of(user);
        assert!(coin::balance<FakeMoney>(user_addr) == 1000, 0);
        deposit_entry<FakeMoney>(user, pool, 1000);
        assert!(coin::balance<FakeMoney>(user_addr) == 0, 1);
        // The coin wrapper's account now has 1000 of the original coins and the pool has 1000 of the wrapper fungible
        // asset.
        assert!(coin::balance<FakeMoney>(coin_wrapper::wrapper_address()) == 1000, 2);
        assert!(fungible_asset::balance(pool) == 1000, 3);
        let recipient_addr = signer::address_of(recipient);
        withdraw_entry<FakeMoney>(user, pool, 1000, recipient_addr);
        assert!(coin::balance<FakeMoney>(coin_wrapper::wrapper_address()) == 0, 4);
        assert!(fungible_asset::balance(pool) == 0, 5);
        assert!(coin::balance<FakeMoney>(recipient_addr) == 1000, 6);
    }
}
