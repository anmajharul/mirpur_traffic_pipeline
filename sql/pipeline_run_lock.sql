create table if not exists pipeline_runtime_locks (
    lock_name text primary key,
    locked_until timestamptz not null default now(),
    locked_by text,
    updated_at timestamptz not null default now()
);

create or replace function try_acquire_pipeline_run_lock(
    p_lock_name text,
    p_owner text,
    p_lease_seconds integer default 900
)
returns boolean
language plpgsql
security definer
as $$
declare
    row_count integer := 0;
begin
    insert into pipeline_runtime_locks (
        lock_name,
        locked_until,
        locked_by,
        updated_at
    )
    values (
        p_lock_name,
        now() + make_interval(secs => greatest(p_lease_seconds, 1)),
        p_owner,
        now()
    )
    on conflict (lock_name) do update
    set
        locked_until = now() + make_interval(secs => greatest(p_lease_seconds, 1)),
        locked_by = p_owner,
        updated_at = now()
    where pipeline_runtime_locks.locked_until <= now();

    get diagnostics row_count = ROW_COUNT;
    return row_count > 0;
end;
$$;

create or replace function release_pipeline_run_lock(
    p_lock_name text,
    p_owner text
)
returns boolean
language plpgsql
security definer
as $$
declare
    row_count integer := 0;
begin
    update pipeline_runtime_locks
    set
        locked_until = now(),
        locked_by = null,
        updated_at = now()
    where lock_name = p_lock_name
      and locked_by = p_owner;

    get diagnostics row_count = ROW_COUNT;
    return row_count > 0;
end;
$$;
