const std = @import("std");

const assert = std.debug.assert;

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

pub const DB = struct {
    memtable: MemTable,

    pub fn LoadFromDir(allocator: std.mem.Allocator, dir_path: []const u8) !DB {
        var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
        defer dir.close();

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();

        var list = std.ArrayList([]const u8).init(arena.allocator());
        var dir_iter = dir.iterate();
        while (try dir_iter.next()) |item| {
            if (item.kind == .file and std.mem.eql(u8, std.fs.path.extension(item.name), ".wal")) {
                try list.append(item.name);
            }
        }

        std.sort.heap([]const u8, list.items, {}, lessThanString);
        var memtable = MemTable.Create(allocator);

        for (list.items) |filename| {
            const path = try std.fmt.allocPrint(arena.allocator(), "{s}/{s}", .{dir_path, filename});

            var wal_iter = try WAL.Iterator.Create(path);
            defer wal_iter.Destroy();

            while (try wal_iter.Next(memtable.arena.allocator())) |entry| {
                memtable.Set(entry.key, entry.value);
            }
        }

        return .{
            .memtable = memtable,
        };
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

    pub fn Get(self: *MemTable, key: []const u8) ?[]const u8 {
        return self.entries.get(key);
    }

    pub fn Set(self: *MemTable, key: []const u8, value: []const u8) void {
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
            var buffer = std.mem.zeroes([8]u8);

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
};

// pub const WAL = struct {
//     file: std.fs.File,
//     buffer: std.io.BufferedWriter(4096, std.fs.File.Writer),

//     pub fn Create(dirname: []const u8) !WAL {
//         try std.fs.cwd().makePath(dirname);

//         const timestamp: u64 = @intCast(std.time.microTimestamp());
//         const filename = try std.fmt.allocPrint(allocator, "{s}/{d}.wal", .{dirname, timestamp});
//         defer allocator.free(filename);

//         const file = try std.fs.cwd().createFile(filename, .{ });
//         const buffer = std.io.bufferedWriter(file.writer());

//         return .{
//             .file = file,
//             .buffer = buffer,
//         };
//     }

//     pub fn Deinit(self: WAL) void {
//         self.file.close();
//     }

//     pub fn WriteEntry(self: *WAL, entry: WALEntry) !void {
//         const writer = self.buffer.writer();

//         const key_len_bytes = std.mem.asBytes(&(@as(u64, entry.key.len)));
//         std.debug.print("{any}\n", .{key_len_bytes});
//         _ = try writer.write(key_len_bytes);
//         _ = try writer.write(entry.key);

//         const val_len_bytes = std.mem.asBytes(&(@as(u64, entry.value.len)));
//         std.debug.print("{any}\n", .{val_len_bytes});
//         _ = try writer.write(val_len_bytes);
//         _ = try writer.write(entry.value);

//         const timestamp_bytes = std.mem.asBytes(&(entry.timestamp));
//         std.debug.print("{any}\n", .{timestamp_bytes});
//         _ = try writer.write(timestamp_bytes);
//     }

//     pub fn Flush(self: *WAL) !void {
//         try self.buffer.flush();
//     }
// };
