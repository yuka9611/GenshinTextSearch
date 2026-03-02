create table avatar
(
    id              integer
        constraint avatar_pk
            primary key autoincrement,
    avatarId        integer,
    nameTextMapHash integer
);

create index avatar_avatarId_index
    on avatar (avatarId);

create table chapter
(
    id                      integer
        constraint chapter_pk
            primary key autoincrement,
    chapterId               integer,
    chapterTitleTextMapHash integer,
    chapterNumTextMapHash   integer
);

create index chapter_chapterId_index
    on chapter (chapterId);

create table dialogue
(
    id         integer
        constraint dialogue_pk
            primary key autoincrement,
    talkerType TEXT,
    talkerId   integer,
    talkId     integer,
    textHash   integer,
    dialogueId integer
        constraint dialogue_pk_2
            unique,
    coopQuestId integer
);

create index dialogue_dialogueId_index
    on dialogue (dialogueId);

create index dialogue_textHash_index
    on dialogue (textHash);


create table fetters
(
    id                       integer
        constraint fetters_pk
            primary key autoincrement,
    fetterId                 integer,
    avatarId                 integer,
    voiceTitleTextMapHash    integer,
    voiceFileTextTextMapHash integer,
    voiceFile                integer
);

create index fetters_voiceFileTextTextMapHash_index
    on fetters (voiceFileTextTextMapHash);

create index fetters_voiceFile_index
    on fetters (voiceFile);

create table fetterStory
(
    id                       integer
        constraint fetterStory_pk
            primary key autoincrement,
    fetterId                 integer,
    avatarId                 integer,
    storyTitleTextMapHash    integer,
    storyTitle2TextMapHash   integer,
    storyTitleLockedTextMapHash integer,
    storyContextTextMapHash  integer,
    storyContext2TextMapHash integer
);

create index fetterStory_avatarId_index
    on fetterStory (avatarId);

create index fetterStory_fetterId_index
    on fetterStory (fetterId);

create index fetterStory_storyContextTextMapHash_index
    on fetterStory (storyContextTextMapHash);

create table langCode
(
    id          integer
        constraint langCode_pk
            primary key autoincrement,
    codeName    TEXT
        constraint langCode_pk_2
            unique,
    displayName TEXT,
    imported    INT
);

create table quest
(
    id               integer
        constraint quest_pk
            primary key autoincrement,
    questId          integer,
    titleTextMapHash integer,
    chapterId        integer,
    created_version_id INTEGER,
    git_created_version_id INTEGER
);

create table quest_version
(
    questId          integer,
    lang             integer,
    updated_version_id INTEGER,
    constraint quest_version_pk
        primary key (questId, lang)
);

create index quest_version_updated_version_id_index
    on quest_version (updated_version_id);

create index quest_questId_index
    on quest (questId);
create unique index quest_questId_uindex
    on quest (questId);

create table questTalk
(
    id      integer
        constraint questTalk_pk
            primary key autoincrement,
    questId integer,
    talkId  integer
);

create index questTalk_talkId_index
    on questTalk (talkId);

create table quest_hash_map
(
    questId     integer not null,
    hash        integer not null,
    source_type TEXT not null,
    constraint quest_hash_map_pk
        primary key (questId, hash, source_type)
);

create index quest_hash_map_hash_index
    on quest_hash_map (hash);
create index quest_hash_map_questId_index
    on quest_hash_map (questId);


create table textMap
(
    id      integer
        constraint textMap_pk
            primary key autoincrement,
    hash    integer,
    content TEXT,
    lang    integer,
    created_version_id INTEGER,
    updated_version_id INTEGER,
    constraint textMap_pk_2
        unique (lang, hash)
);

-- 由于textMap表已有(lang, hash)的唯一约束，单独的hash和lang索引是冗余的
-- 唯一约束会自动创建索引，因此删除这些冗余索引


create table voice
(
    id          integer
        constraint voice_pk
            primary key autoincrement,
    dialogueId  integer,
    voicePath   TEXT,
    gameTrigger TEXT,
    avatarId    integer
);

create index voice_dialogueId_index
    on voice (dialogueId);


create table npc
(
    id       integer
        constraint npc_pk
            primary key autoincrement,
    npcId    integer
        constraint npc_pk_2
            unique,
    textHash integer
);

create index npc_npcId_index
    on npc (npcId);


create table manualTextMap
(
    id        integer
        constraint manualTextMap_pk
            primary key,
    textMapId text,
    textHash  integer
);

create unique index manualTextMap_textMapId_uindex
    on manualTextMap (textMapId);

create table readable
(
    id       integer
        constraint readable_pk
            primary key autoincrement,
    fileName text,
    lang     text,
    content  text,
    titleTextMapHash integer,
    readableId integer,
    created_version_id INTEGER,
    updated_version_id INTEGER,
    constraint readable_pk_2
        unique (fileName, lang)
);

create index readable_fileName_index
    on readable (fileName);

create index readable_lang_index
    on readable (lang);

create index readable_lang_fileName_index
    on readable (lang, fileName);

create index readable_readableId_index
    on readable (readableId);

create table subtitle
(
    id        integer
        constraint subtitle_pk
            primary key autoincrement,
    fileName  text,
    lang      integer,
    startTime real,
    endTime   real,
    content   text,
    subtitleId integer,
    subtitleKey TEXT,
    created_version_id INTEGER,
    updated_version_id INTEGER
);

create index subtitle_fileName_index
    on subtitle (fileName);

create index subtitle_lang_index
    on subtitle (lang);

create index subtitle_fileName_lang_startTime_index
    on subtitle (fileName, lang, startTime);

create index subtitle_lang_subtitleId_startTime_index
    on subtitle (lang, subtitleId, startTime);

create index subtitle_subtitleId_index
    on subtitle (subtitleId);
create unique index subtitle_subtitleKey_uindex
    on subtitle (subtitleKey);

create table version_catalog
(
    source_table TEXT not null,
    raw_version TEXT not null,
    version_tag TEXT,
    updated_at TEXT default (datetime('now')) not null,
    constraint version_catalog_pk
        primary key (source_table, raw_version)
);

create index version_catalog_source_version_tag_index
    on version_catalog (source_table, version_tag);

create index version_catalog_version_tag_index
    on version_catalog (version_tag);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (1, 'TextMapCHS.json', '简体中文', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (2, 'TextMapCHT.json', '繁體中文', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (3, 'TextMapDE.json', 'Deutsch', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (4, 'TextMapEN.json', 'English', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (5, 'TextMapES.json', 'Español', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (6, 'TextMapFR.json', 'Français', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (7, 'TextMapID.json', 'Bahasa Indonesia', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (8, 'TextMapIT.json', 'Italiano', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (9, 'TextMapJP.json', '日本語', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (10, 'TextMapKR.json', '한국어', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (11, 'TextMapPT.json', 'Português', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (12, 'TextMapRU.json', 'Русский язык', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (13, 'TextMapTH.json', 'ภาษาไทย', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (14, 'TextMapTR.json', 'Türkçe', 0);
INSERT INTO langCode (id, codeName, displayName, imported) VALUES (15, 'TextMapVI.json', 'Tiếng Việt', 0);



create table version_dim
(
    id integer
        constraint version_dim_pk
            primary key autoincrement,
    raw_version TEXT not null
        constraint version_dim_raw_version_uindex
            unique,
    version_tag TEXT
);

create unique index version_dim_raw_version_uindex
    on version_dim (raw_version);

create index version_dim_version_tag_index
    on version_dim (version_tag);



create index textMap_created_version_id_index
    on textMap (created_version_id);
create index textMap_updated_version_id_index
    on textMap (updated_version_id);
create index quest_created_version_id_index
    on quest (created_version_id);
create index quest_git_created_version_id_index
    on quest (git_created_version_id);
create index readable_created_version_id_index
    on readable (created_version_id);
create index readable_updated_version_id_index
    on readable (updated_version_id);
create index subtitle_created_version_id_index
    on subtitle (created_version_id);
create index subtitle_updated_version_id_index
    on subtitle (updated_version_id);
