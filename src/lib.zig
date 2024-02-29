const std = @import("std");

const assert = std.debug.assert;

fn micro_time() u64 {
    return @intCast(std.time.microTimestamp());
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

// Size a MemTable can grow to before being flushed to disk
const MEMTABLE_THRESHOLD_BYTES = 1030;
// The fraction of entries included in the index file e.g. 5 = every 5th entry
const INDEX_SPARSE_FRACTION = 5;
comptime {
    assert(MEMTABLE_THRESHOLD_BYTES > 0);
    assert(INDEX_SPARSE_FRACTION > 0);
}

pub const DB = struct {
    root_path: []const u8,
    allocator: std.mem.Allocator,
    memtable: MemTable,
    wal_writer: WAL.Writer,

    pub fn LoadFromDir(allocator: std.mem.Allocator, dir_path: []const u8) !DB {
        var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
        defer dir.close();

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var list = std.ArrayList([]const u8).init(arena.allocator());
        var dir_iter = dir.iterate();
        while (try dir_iter.next()) |item| {
            if (item.kind == .file and std.mem.eql(u8, std.fs.path.extension(item.name), ".wal")) {
                const path = std.fmt.allocPrint(arena.allocator(), "{s}/{s}", .{ dir_path, item.name }) catch unreachable;
                try list.append(path);
            }
        }

        std.sort.heap([]const u8, list.items, {}, lessThanString);

        var memtable = MemTable.Create(allocator);
        var wal_writer = try WAL.Writer.Create(arena.allocator(), dir_path);

        for (list.items) |file_path| {
            var wal_iter = try WAL.Iterator.Create(file_path);
            defer wal_iter.Destroy();

            while (try wal_iter.Next(memtable.arena.allocator())) |entry| {
                // TODO should this use DB.Set so it flushes the memtable?
                try wal_writer.WriteEntry(entry);
                memtable.Set(entry.key, entry.value);
            }
        }

        try wal_writer.Flush();

        for (list.items) |file_path| {
            try std.fs.cwd().deleteFile(file_path);
        }

        return .{
            .root_path = dir_path,
            .allocator = allocator,
            .memtable = memtable,
            .wal_writer = wal_writer,
        };
    }

    pub fn Close(self: *DB) void {
        self.memtable.Destroy();
        self.wal_writer.Destroy();
    }

    pub fn Put(self: *DB, key: []const u8, value: []const u8) !void {
        try self.wal_writer.WriteEntry(.{ .key = key, .value = value, .timestamp = micro_time() });
        try self.wal_writer.Flush();
        self.memtable.Set(key, value);

        if (self.memtable.Size() > MEMTABLE_THRESHOLD_BYTES) {
            try self.flush_memtable();
        }
    }

    fn flush_memtable(self: *DB) !void {
        std.debug.print("Flushing MemTable!\n", .{});

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const path_buffer = arena.allocator().alloc(u8, self.root_path.len + 1 + 19 + 4 + 1) catch unreachable;
        const timestamp = micro_time();

        const db_path = std.fmt.bufPrintZ(path_buffer, "{s}/{}.db", .{self.root_path, timestamp}) catch unreachable;
        var db_file = try std.fs.cwd().createFileZ(db_path, .{});
        defer db_file.close();
        var db_buffer = std.io.bufferedWriter(db_file.writer());
        const db_writer = db_buffer.writer();

        const index_path = std.fmt.bufPrintZ(path_buffer, "{s}/{}.dbi", .{self.root_path, timestamp}) catch unreachable;
        var index_file = try std.fs.cwd().createFileZ(index_path, .{});
        defer index_file.close();
        var index_buffer = std.io.bufferedWriter(index_file.writer());
        const index_writer = index_buffer.writer();

        // TODO: Remove when we use a pre sorted memtable
        const keys = self.memtable.entries.keys();
        std.sort.heap([]const u8, keys, {}, lessThanString);
        try self.memtable.entries.reIndex(arena.allocator());

        var cursor: usize = 0;
        for (keys, 0..) |key, i| {
            const value = self.memtable.Get(key).?;
            cursor += key.len;

            if (i % INDEX_SPARSE_FRACTION == 0) {
                try index_writer.writeAll(std.mem.asBytes(&(@as(u64, key.len))));
                try index_writer.writeAll(key);
                try index_writer.writeAll(std.mem.asBytes(&(@as(u64, cursor))));
                try index_writer.writeAll(std.mem.asBytes(&(@as(u64, value.len))));
            }

            try db_writer.writeAll(key);
            try db_writer.writeAll(value);

            cursor += value.len;
        }
        assert(cursor == self.memtable.Size());
        try db_buffer.flush();
        try index_buffer.flush();

        self.memtable.Reset();
    }

    pub fn Get(self: *DB, key: []const u8) !?[]const u8 {
        if (self.memtable.Get(key)) |value|  {
            return value;
        }

        var dir = try std.fs.cwd().openDir(self.root_path, .{ .iterate = true });
        defer dir.close();

        var dir_iter = dir.iterate();
        while (try dir_iter.next()) |item| {
            if (item.kind == .file and std.mem.eql(u8, std.fs.path.extension(item.name), ".dbi")) {
                var index_file = try dir.openFile(item.name, .{});
                defer index_file.close();

                var arena = std.heap.ArenaAllocator.init(self.allocator);
                defer arena.deinit();

                var index = Index.Init(&index_file);
                if (try index.Search(key, arena.allocator())) |entry| {
                    std.debug.print("Search returned: {}\n", .{entry});
                    // TODO fix this hacky way to get the file name
                    const db_file = try dir.openFile(item.name[0..item.name.len-1], .{});
                    defer db_file.close();

                    assert(entry.offset + entry.length < (try db_file.stat()).size);
                    try db_file.seekTo(entry.offset);
                    // TODO think about this allocation
                    const value = self.allocator.alloc(u8, entry.length) catch unreachable;
                    const read = try db_file.reader().read(value);
                    assert(read == entry.length); // TODO clean up asserts that aren't really assertions
                    return value;
                }
                // TODO read multiple index files
                break;
            }
        }

        return null;
    }
};

const MemTable = struct {
    arena: std.heap.ArenaAllocator,
    // TODO: not use a hashmap
    entries: std.StringArrayHashMapUnmanaged([]const u8),
    size: usize,

    pub fn Create(allocator: std.mem.Allocator) MemTable {
        return .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .entries = std.StringArrayHashMapUnmanaged([]const u8){},
            .size = 0,
        };
    }

    pub fn Destroy(self: *MemTable) void {
        self.arena.deinit();
    }

    pub fn Reset(self: *MemTable) void {
        // TODO look at this
        self.arena.deinit();
        self.arena = std.heap.ArenaAllocator.init(self.arena.child_allocator);
        self.entries = std.StringArrayHashMapUnmanaged([]const u8){};
        self.size = 0;
    }

    pub fn Size(self: *MemTable) usize {
        return self.size;
    }

    pub fn Get(self: *MemTable, key: []const u8) ?[]const u8 {
        return self.entries.get(key);
    }

    pub fn Set(self: *MemTable, key: []const u8, value: []const u8) void {
        // TODO this is exact size - could we use a heuristic that is pessimistic instead?
        // That could avoid the need to lookup the value
        var entry_size = key.len + value.len;
        if (self.entries.contains(key)) {
            entry_size -= (key.len + self.entries.get(key).?.len);
        }

        self.size += entry_size;
        assert(self.size > 0);
        self.entries.put(self.arena.allocator(), key, value) catch unreachable;
    }
};

const WAL = struct {
    pub const Entry = struct {
        key: []const u8,
        value: []const u8,
        timestamp: u64,
    };

    pub const Iterator = struct {
        file: std.fs.File,
        buffered_reader: std.io.BufferedReader(4096, std.fs.File.Reader),

        pub fn Create(dir_path: []const u8) !Iterator {
            const file = try std.fs.cwd().createFile(dir_path, .{ .read = true, .truncate = false });
            const buffered_reader = std.io.bufferedReader(file.reader());

            return .{
                .file = file,
                .buffered_reader = buffered_reader,
            };
        }

        pub fn Destroy(self: *Iterator) void {
            self.file.close();
        }

        pub fn Next(self: *Iterator, allocator: std.mem.Allocator) !?Entry {
            const reader = self.buffered_reader.reader();
            var buffer = std.mem.zeroes([@sizeOf(u64)]u8);
            comptime assert(buffer.len == @sizeOf(u64));

            // Key Length: 8 bytes
            var bytes_read = try reader.read(&buffer);
            // Check EOF
            if (bytes_read == 0) return null;
            assert(bytes_read == buffer.len);
            const key_len = std.mem.readVarInt(u64, &buffer, .little);
            assert(key_len > 0);

            // Key: `key_len` bytes
            const key = allocator.alloc(u8, key_len) catch unreachable;
            bytes_read = try reader.read(key);
            assert(bytes_read == key_len);

            // Value Length: 8 bytes
            bytes_read = try reader.read(&buffer);
            assert(bytes_read == buffer.len);
            const val_len = std.mem.readInt(u64, &buffer, .little);
            assert(val_len > 0);

            // Value: `val_len` bytes
            const value = allocator.alloc(u8, val_len) catch unreachable;
            bytes_read = try reader.read(value);
            assert(bytes_read == val_len);

            // Timestamp: 8 bytes
            bytes_read = try reader.read(&buffer);
            assert(bytes_read == buffer.len);
            const timestamp = std.mem.readInt(u64, &buffer, .little);

            return .{
                .key = key,
                .value = value,
                .timestamp = timestamp,
            };
        }
    };

    pub const Writer = struct {
        file: std.fs.File,
        buffered_writer: std.io.BufferedWriter(4096, std.fs.File.Writer),

        pub fn Create(allocator: std.mem.Allocator, dir_path: []const u8) !Writer {
            try std.fs.cwd().makePath(dir_path);

            const timestamp = micro_time();
            const filename = try std.fmt.allocPrint(allocator, "{s}/{d}.wal", .{ dir_path, timestamp });
            defer allocator.free(filename);

            const file = try std.fs.cwd().createFile(filename, .{});
            const buffered_writer = std.io.bufferedWriter(file.writer());

            return .{
                .file = file,
                .buffered_writer = buffered_writer,
            };
        }

        pub fn Destroy(self: *Writer) void {
            self.file.close();
        }

        pub fn WriteEntry(self: *Writer, entry: Entry) !void {
            const writer = self.buffered_writer.writer();

            const key_len_bytes = std.mem.asBytes(&(@as(u64, entry.key.len)));
            var bytes_written = try writer.write(key_len_bytes);
            assert(bytes_written == key_len_bytes.len);
            bytes_written = try writer.write(entry.key);
            assert(bytes_written == entry.key.len);

            const val_len_bytes = std.mem.asBytes(&(@as(u64, entry.value.len)));
            bytes_written = try writer.write(val_len_bytes);
            assert(bytes_written == val_len_bytes.len);
            bytes_written = try writer.write(entry.value);
            assert(bytes_written == entry.value.len);

            const timestamp_bytes = std.mem.asBytes(&(entry.timestamp));
            bytes_written = try writer.write(timestamp_bytes);
            assert(bytes_written == timestamp_bytes.len);
        }

        pub fn Flush(self: *Writer) !void {
            try self.buffered_writer.flush();
        }
    };
};

const Index = struct {
    file: *std.fs.File,

    pub fn Init(file: *std.fs.File) Index {
        return .{
            .file = file,
        };
    }

    const Entry = struct {
        offset: usize,
        length: usize,
    };

    pub fn Search(self: *Index, key: []const u8, allocator: std.mem.Allocator) !?Entry {
        const bytes = try self.file.readToEndAlloc(allocator, std.math.maxInt(u64));
        defer allocator.free(bytes);
        assert(bytes.len > 0);

        var cursor: usize = 0;
        var prev: ?Entry = null;
        while (true) {
            if (cursor >= bytes.len) return null;

            // Key Length: 8 bytes
            var step: usize = @sizeOf(u64);
            assert(cursor + step < bytes.len);
            const key_len_buffer = bytes[cursor..cursor+step];
            const key_len = std.mem.readVarInt(u64, key_len_buffer, .little);
            assert(key_len > 0);
            cursor += step;

            // Key: Length `key_len` bytes
            step = key_len;
            assert(cursor + step < bytes.len);
            const index_key = bytes[cursor..cursor+step];
            cursor += step;

            // Offset: 8 bytes
            step = @sizeOf(u64);
            assert(cursor + step < bytes.len);
            const offset_buffer = bytes[cursor..cursor+step];
            const offset = std.mem.readVarInt(u64, offset_buffer, .little);
            cursor += step;

            // Value Length: 8 bytes
            step = @sizeOf(u64);
            assert(cursor + step < bytes.len);
            const length_buffer = bytes[cursor..cursor+step];
            const length = std.mem.readVarInt(u64, length_buffer, .little);
            cursor += step;

            std.debug.print("Found key: {s}\n", .{index_key});
            const entry = Entry{ .offset = offset, .length = length };
            switch (std.mem.order(u8, index_key, key)) {
                .lt => prev = entry,
                .eq => return entry,
                .gt => return prev,
            }
        }
    }
};
