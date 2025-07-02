create table loans
(
    id                  uuid                                             not null
        primary key,
    member_id           uuid                                             not null,
    amount              double precision default 0                       not null,
    created_by          uuid                                             not null,
    created_date        timestamp        default CURRENT_TIMESTAMP       not null,
    language            varchar(10)      default 'en'::character varying not null,
    last_updated_date   timestamp        default CURRENT_TIMESTAMP       not null,
    last_updated_by     uuid                                             not null,
    tenant_id           uuid                                             not null,
    vertical_id         uuid                                             not null,
    service_id          varchar(255)                                     not null,
    is_marked_to_delete boolean          default false                   not null,
    version             integer          default 0                       not null
);

alter table loans
    owner to postgres;

create index idx_loans_tenant_id
    on loans (tenant_id);

create index idx_loans_member_id
    on loans (member_id);

create table spring_boot_messages
(
    id           bigserial
        primary key,
    message_id   varchar(255) not null,
    content      text         not null,
    timestamp    timestamp default CURRENT_TIMESTAMP,
    processed_at timestamp default CURRENT_TIMESTAMP
);

alter table spring_boot_messages
    owner to postgres;

create table dotnet_messages
(
    id           bigserial
        primary key,
    message_id   varchar(255) not null,
    content      text         not null,
    timestamp    timestamp default CURRENT_TIMESTAMP,
    processed_at timestamp default CURRENT_TIMESTAMP
);

alter table dotnet_messages
    owner to postgres;

