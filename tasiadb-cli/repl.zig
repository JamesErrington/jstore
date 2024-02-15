const std = @import("std");
const build = @import("build");

const version_str = "v" ++ build.version ++ " (" ++ build.date ++ ")";

pub const Repl = struct {
    should_exit: bool,
    fout: std.fs.File.Writer,
    ferr: std.fs.File.Writer,
    fin: std.fs.File.Reader,

    const Self = @This();

    pub fn start(arena: *std.heap.ArenaAllocator) !void {
        const allocator = arena.allocator();

        var repl = Self{
            .should_exit = false,
            .fout = std.io.getStdOut().writer(),
            .ferr = std.io.getStdErr().writer(),
            .fin = std.io.getStdIn().reader(),
        };

        try repl.display_header();
        while (!repl.should_exit) {
            var exec_arena = std.heap.ArenaAllocator.init(allocator);
            defer exec_arena.deinit();

            try repl.run(&exec_arena);
        }
    }

    fn run(repl: *Self, arena: *std.heap.ArenaAllocator) !void {
        try repl.fout.print("> ", .{});

        var reader = std.io.bufferedReader(repl.fin);
        const stream = reader.reader();

        var input = std.ArrayList(u8).init(arena.allocator());
        try stream.streamUntilDelimiter(input.writer(), '\n', 128);

        if (std.mem.eql(u8, ".exit", input.items)) {
            std.process.exit(0);
        }

        try repl.fout.print("You input: '{s}'\n", .{input.items});
    }

    fn display_header(repl: *const Self) !void {
        try repl.fout.print("tasiadb " ++ version_str ++ "\n", .{});
    }
};
