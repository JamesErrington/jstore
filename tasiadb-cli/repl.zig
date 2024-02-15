const std = @import("std");
const build = @import("config");

const version_str = "v" ++ build.version ++ " (" ++ build.date ++ ")";

pub const Repl = struct {
	fout: std.fs.File.Writer,
	ferr: std.fs.File.Writer,

	const Self = @This();

	pub fn run() !void {
		const repl = Self {
			.fout = std.io.getStdOut().writer(),
			.ferr = std.io.getStdErr().writer(),
		};

		try repl.display_header();
	}

	fn display_header(repl: *const Self) !void {
		try repl.fout.print("tasiadb " ++ version_str ++ "\n", .{});
	}
};
