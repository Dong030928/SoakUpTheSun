package org.hgc.suts.gateway.memory.archive;

class JdbcConversationArchiveTest extends ArchiveContract {
    @Override protected ArchiveDatabaseFixture database() { return ArchiveDatabaseFixture.h2(); }
}
