const std = @import("std");

const assert = std.debug.assert;

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

pub const DB = struct {
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
                try wal_writer.WriteEntry(entry);
                memtable.Set(entry.key, entry.value);
            }
        }

        try wal_writer.Flush();

        for (list.items) |file_path| {
            try std.fs.cwd().deleteFile(file_path);
        }

        return .{
            .memtable = memtable,
            .wal_writer = wal_writer,
        };
    }

    pub fn Close(self: *DB) void {
        self.memtable.Destroy();
        self.wal_writer.Destroy();
    }
};

const MemTable = struct {
    arena: std.heap.ArenaAllocator,
    entries: std.StringHashMapUnmanaged([]const u8),
    size: usize,
    frozen: bool,

    pub fn Create(allocator: std.mem.Allocator) MemTable {
        return .{
            .arena = std.heap.ArenaAllocator.init(allocator),
            .entries = std.StringHashMapUnmanaged([]const u8){},
            .size = 0,
            .frozen = false,
        };
    }

    pub fn Destroy(self: *MemTable) void {
        self.arena.deinit();
    }

    pub fn Size(self: *MemTable) usize {
        return self.size;
    }

    pub fn Get(self: *MemTable, key: []const u8) ?[]const u8 {
        return self.entries.get(key);
    }

    pub fn Set(self: *MemTable, key: []const u8, value: []const u8) void {
        self.size += key.len + value.len;
        self.entries.put(self.arena.allocator(), key, value) catch unreachable;
    }

    pub fn UpdateSize(self: *MemTable) void {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            self.size += entry.key_ptr.*.len + entry.value_ptr.*.len;
        }
    }

    pub fn Freeze(self: *MemTable) void {
        self.frozen = true;
    }

    pub fn IsFrozen(self: *MemTable) bool {
        return self.frozen;
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

            const timestamp: u64 = @intCast(std.time.microTimestamp());
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
