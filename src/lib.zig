const std = @import("std");

const ZipTree = @import("./storage/ziptree.zig");

const allocator = std.heap.c_allocator;
const assert = std.debug.assert;

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

pub const DB = struct {
    dirname: []const u8,

    const OpenOptions = struct {
        dirname: []const u8,
    };

    pub fn Open(options: OpenOptions) *DB {
        _ = options;
    }

    pub fn LoadFromDir(dir_path: []const u8) !MemTable {
        var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
        defer dir.close();

        var list = std.ArrayList([]const u8).init(allocator);
        defer list.deinit();

        var iter = dir.iterate();
        while (try iter.next()) |item| {
            if (item.kind != .file or std.mem.eql(u8, std.fs.path.extension(item.name), ".wal") == false) continue;
            try list.append(item.name);
        }

        std.sort.heap([]const u8, list.items, {}, lessThanString);

        var memtable = MemTable.Create();
        var wal = try WAL.Create(dir_path);

        for (list.items) |filename| {
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{dir_path, filename});
            defer allocator.free(path);

            var wal_iter = try WALIterator.Create(path);
            defer wal_iter.deinit();

            while (wal_iter.next()) |entry| {
                _ = try memtable.Set(entry.key, entry.value);
                try wal.WriteEntry(entry);
            }
        }

        try wal.Flush();

        for (list.items) |filename| {
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{dir_path, filename});
            defer allocator.free(path);

            try std.fs.cwd().deleteFile(path);
        }

        return memtable;
    }
};

const MemTable = struct {
    entries: ZipTree,
    size: usize,

    pub fn Create() MemTable {
        return .{
            .entries = ZipTree.init(allocator, 0),
            .size = 0,
        };
    }

    pub fn Get(self: *MemTable, key: []const u8) ?[]const u8 {
        return self.entries.search(key);
    }

    pub fn Set(self: *MemTable, key: []const u8, value: []const u8) !bool {
        return self.entries.insert(key, value);
    }

    pub fn Delete(self: *MemTable, key: []const u8) bool {
        return self.entries.delete(key);
    }
};

const WALEntry = struct {
    key: []const u8,
    value: []const u8,
    timestamp: u64,
};

pub const WALIterator = struct {
    file: std.fs.File,
    buffer: std.io.BufferedReader(4096, std.fs.File.Reader),

    pub fn Create(path: []const u8) !WALIterator {
        const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = false });
        const buffer = std.io.bufferedReader(file.reader());

        return .{
            .file = file,
            .buffer = buffer,
        };
    }

    pub fn deinit(self: WALIterator) void {
        self.file.close();
    }

    pub fn next(self: *WALIterator) ?WALEntry {
        const reader = self.buffer.reader();
        var buffer = std.mem.zeroes([8]u8);

        var read = reader.read(&buffer) catch 0;
        std.debug.print("KEY LEN BYTES: {}\n", .{read});
        if (read != buffer.len) return null;
        const key_len = std.mem.readVarInt(u64, &buffer, .little);
        std.debug.print("KEY LEN: {}\n", .{key_len});
        assert(key_len > 0);


        const key_buf = allocator.alloc(u8, key_len) catch return null;
        read = reader.read(key_buf) catch 0;
        std.debug.print("KEY BYTES: {}\n", .{read});
        if (read != key_buf.len) return null;
        std.debug.print("KEY: {s}\n", .{key_buf});

        read = reader.read(&buffer) catch 0;
        std.debug.print("VAL LEN BYTES: {}\n", .{read});
        if (read != buffer.len) return null;
        const val_len = std.mem.readInt(u64, &buffer, .little);
        std.debug.print("VAL LEN: {}\n", .{val_len});
        assert(val_len > 0);

        const val_buf = allocator.alloc(u8, val_len) catch return null;
        read = reader.read(val_buf) catch 0;
        std.debug.print("VAL BYTES: {}\n", .{read});
        if (read != val_buf.len) return null;
         std.debug.print("VAL: {s}\n", .{val_buf});

        read = reader.read(&buffer) catch 0;
        std.debug.print("TIME BYTES: {}\n", .{read});
        if (read != buffer.len) return null;
        const timestamp = std.mem.readInt(u64, &buffer, .little);
        std.debug.print("TIMESTAMP: {}\n", .{timestamp});

        return .{
            .key = key_buf,
            .value = val_buf,
            .timestamp = timestamp,
        };
    }
};

pub const WAL = struct {
    file: std.fs.File,
    buffer: std.io.BufferedWriter(4096, std.fs.File.Writer),

    pub fn Create(dirname: []const u8) !WAL {
        try std.fs.cwd().makePath(dirname);

        const timestamp: u64 = @intCast(std.time.microTimestamp());
        const filename = try std.fmt.allocPrint(allocator, "{s}/{d}.wal", .{dirname, timestamp});
        defer allocator.free(filename);

        const file = try std.fs.cwd().createFile(filename, .{ });
        const buffer = std.io.bufferedWriter(file.writer());

        return .{
            .file = file,
            .buffer = buffer,
        };
    }

    pub fn Deinit(self: WAL) void {
        self.file.close();
    }

    pub fn WriteEntry(self: *WAL, entry: WALEntry) !void {
        const writer = self.buffer.writer();

        const key_len_bytes = std.mem.asBytes(&(@as(u64, entry.key.len)));
        std.debug.print("{any}\n", .{key_len_bytes});
        _ = try writer.write(key_len_bytes);
        _ = try writer.write(entry.key);

        const val_len_bytes = std.mem.asBytes(&(@as(u64, entry.value.len)));
        std.debug.print("{any}\n", .{val_len_bytes});
        _ = try writer.write(val_len_bytes);
        _ = try writer.write(entry.value);

        const timestamp_bytes = std.mem.asBytes(&(entry.timestamp));
        std.debug.print("{any}\n", .{timestamp_bytes});
        _ = try writer.write(timestamp_bytes);
    }

    pub fn Flush(self: *WAL) !void {
        try self.buffer.flush();
    }
};
