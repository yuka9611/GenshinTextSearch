create table public.user_preferences (
  user_id uuid primary key references auth.users(id) on delete cascade,
  result_languages integer[] not null default array[1],
  default_search_language integer not null default 1,
  source_language integer not null default 1,
  traveler_mode text not null default 'both',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint user_preferences_result_languages_count
    check (cardinality(result_languages) between 1 and 20),
  constraint user_preferences_result_languages_values
    check (
      array_position(result_languages, null) is null
      and 0 <= all(result_languages)
      and 100 >= all(result_languages)
    ),
  constraint user_preferences_default_language_range
    check (default_search_language between 0 and 100),
  constraint user_preferences_source_language_range
    check (source_language between 0 and 100),
  constraint user_preferences_traveler_mode
    check (traveler_mode in ('male', 'female', 'both'))
);

create table public.search_history (
  id bigint generated always as identity primary key,
  user_id uuid not null references auth.users(id) on delete cascade,
  search_type varchar(32) not null,
  keyword varchar(500) not null default '',
  filters jsonb not null default '{}'::jsonb,
  result_count integer,
  created_at timestamptz not null default now(),
  constraint search_history_search_type
    check (search_type in (
      'text',
      'name',
      'npc_dialogue',
      'voice',
      'story',
      'catalog'
    )),
  constraint search_history_filters_object
    check (jsonb_typeof(filters) = 'object'),
  constraint search_history_result_count
    check (result_count is null or result_count >= 0)
);

create index search_history_user_created_idx
  on public.search_history (user_id, created_at desc);

alter table public.user_preferences enable row level security;
alter table public.search_history enable row level security;

create policy "users_select_own_preferences"
  on public.user_preferences for select
  to authenticated
  using ((select auth.uid()) = user_id);

create policy "users_insert_own_preferences"
  on public.user_preferences for insert
  to authenticated
  with check ((select auth.uid()) = user_id);

create policy "users_update_own_preferences"
  on public.user_preferences for update
  to authenticated
  using ((select auth.uid()) = user_id)
  with check ((select auth.uid()) = user_id);

create policy "users_delete_own_preferences"
  on public.user_preferences for delete
  to authenticated
  using ((select auth.uid()) = user_id);

create policy "users_select_own_search_history"
  on public.search_history for select
  to authenticated
  using ((select auth.uid()) = user_id);

create policy "users_insert_own_search_history"
  on public.search_history for insert
  to authenticated
  with check ((select auth.uid()) = user_id);

create policy "users_delete_own_search_history"
  on public.search_history for delete
  to authenticated
  using ((select auth.uid()) = user_id);

revoke all on table public.user_preferences from anon;
revoke all on table public.search_history from anon;
grant select, insert, update, delete on table public.user_preferences to authenticated;
grant select, insert, delete on table public.search_history to authenticated;
grant usage, select on sequence public.search_history_id_seq to authenticated;
