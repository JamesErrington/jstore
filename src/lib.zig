const std = @import("std");

const ZipTree = @import("./storage/ziptree.zig");

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
        var len_buf: [8]u8 = [_]u8{0} ** 8;

        var read = reader.read(&len_buf) catch 0;
        if (read != len_buf.len) return null;
        const key_len = std.mem.readVarInt(u64, &len_buf, .little);

        // _ = self.reader.read(&len_buf) catch {
        //     return null;
        // };
        // const val_len = std.mem.readVarInt(u64, &len_buf, .little);

        const key_buf = std.heap.c_allocator.alloc(u8, key_len) catch return null;
        read = reader.read(key_buf) catch 0;
        if (read != key_buf.len) return null;

        return .{
            .key = key_buf,
            .value = "",
            .timestamp = 0,
        };
    }
};
