const std = @import("std");
const httpz = @import("httpz");
const logz = @import("logz");
const uuid = @import("zul").UUID;
const db = @import("db.zig");

const Context = struct {
    // database: db.DbConnection, // TODO: add db for persistence
    cache: *db.DbConnection,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    try logz.setup(allocator, .{
        .level = .Info,
        .pool_size = 100,
        .buffer_size = 4096,
        .large_buffer_count = 8,
        .large_buffer_size = 16384,
        .output = .stdout,
        .encoding = .logfmt,
    });
    defer logz.deinit();

    var env = try std.process.getEnvMap(allocator);
    defer env.deinit();
    const api_port = try std.fmt.parseUnsigned(u16, env.get("API_PORT") orelse "5882", 10);
    const redis_port = try std.fmt.parseUnsigned(u16, env.get("REDIS_PORT") orelse "6379", 10);
    const redis_host = env.get("REDIS_HOST") orelse "127.0.0.1";

    var redis = db.RedisConnection.init(allocator);
    defer redis.deinit();
    const redis_host_z = try allocator.dupeZ(u8, redis_host);
    defer allocator.free(redis_host_z);
    try redis.connect(redis_host_z, redis_port);
    logz.info()
        .string("message", "redis connected")
        .string("host", redis_host)
        .int("port", redis_port).log();

    var dbConn = db.DbConnection{ .redis = redis };
    const context = Context{ .cache = &dbConn };
    var server = try httpz.ServerCtx(Context, Context).init(
        allocator,
        .{ .port = api_port },
        context,
    );

    server.dispatcher(logDispatcher);
    server.errorHandler(errorHandler);

    var router = server.router();

    router.get("/user/:id", getUser);
    router.post("/user", postUser);

    logz.info().string("details", "server listening").int("port", api_port).log();
    try server.listen();
}

fn logDispatcher(ctx: Context, action: httpz.Action(Context), req: *httpz.Request, res: *httpz.Response) !void {
    var timer = try std.time.Timer.start();
    try action(ctx, req, res);
    const elapsed = timer.read();
    logz.info()
        .string("path", req.url.path)
        .string("method", @tagName(req.method))
        .int("status", res.status)
        .int("elapsed", elapsed).log();
}

fn errorHandler(_: Context, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
    res.status = 500;
    res.body = "Internal Server Error";
    logz.warn()
        .string("details", "unhandled error")
        .err(err)
        .string("path", req.url.path)
        .string("method", @tagName(req.method))
        .int("status", res.status).log();
}

fn postUser(ctx: Context, _: *httpz.Request, res: *httpz.Response) !void {
    const id = uuid.v4();
    const user = db.User{ .id = id };
    ctx.cache.redis.setUser(user) catch |err| {
        if (err == db.RedisConnection.errors.FailedToSet) {
            res.status = 400;
            return;
        }
        return err;
    };
    res.status = 201;
    try res.json(user, .{});
}

fn getUser(ctx: Context, req: *httpz.Request, res: *httpz.Response) !void {
    const id = req.param("id") orelse "";
    const parsed_id = uuid.parse(id) catch null;
    if (parsed_id == null) {
        res.status = 400;
        res.body = "Invalid id";
        logz.err().string("id", id).log();
        return;
    }
    const user = try ctx.cache.redis.getUser(id);
    if (user == null) {
        res.status = 404;
        res.body = "User not found";
    } else {
        res.status = 200;
        try res.json(user, .{});
    }
}
