const std = @import("std");

const ZipTree = @import("./storage/ziptree.zig");

const assert = std.debug.assert;

const DB = struct {
    dirname: []const u8,

    const OpenOptions = struct {
        dirname: []const u8,
    };

    pub fn Open(options: OpenOptions) *DB {
        _ = options;
    }
};

const MemTable = struct {
    entries: ZipTree,
    size: usize,

    pub fn Create() MemTable {
        return .{
            .entries = ZipTree.init(std.heap.c_allocator, 0),
            .size = 0,
        };
    }

    pub fn Get(self: *const MemTable, key: []const u8) ?[]const u8 {
        return self.entries.search(key);
    }

    pub fn Set(self: *const MemTable, key: []const u8, value: []const u8) !bool {
        return self.entries.insert(key, value);
    }

    pub fn Delete(self: *const MemTable, key: []const u8) bool {
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

    pub fn init(path: []const u8) !WALIterator {
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
        if (read != buffer.len) return null;
        const key_len = std.mem.readVarInt(u64, &buffer, .little);
        assert(key_len > 0);

        const key_buf = std.heap.c_allocator.alloc(u8, key_len) catch return null;
        read = reader.read(key_buf) catch 0;
        if (read != key_buf.len) return null;

        read = reader.read(&buffer) catch 0;
        if (read != buffer.len) return null;
        const val_len = std.mem.readInt(u64, &buffer, .little);
        assert(val_len > 0);

        const val_buf = std.heap.c_allocator.alloc(u8, val_len) catch return null;
        read = reader.read(val_buf) catch 0;
        if (read != val_buf.len) return null;

        read = reader.read(&buffer) catch 0;
        if (read != buffer.len) return null;
        const timestamp = std.mem.readInt(u64, &buffer, .little);

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

    pub fn init(dirname: []const u8) !WAL {
        try std.fs.cwd().makePath(dirname);

        const timestamp: u64 = @intCast(std.time.microTimestamp());
        const filename = try std.fmt.allocPrint(std.heap.c_allocator, "{s}/{d}.wal", .{dirname, timestamp});
        defer std.heap.c_allocator.free(filename);

        const file = try std.fs.cwd().createFile(filename, .{ });
        const buffer = std.io.bufferedWriter(file.writer());

        return .{
            .file = file,
            .buffer = buffer,
        };
    }

    pub fn deinit(self: WAL) void {
        self.file.close();
    }
};
