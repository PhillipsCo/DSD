using DSD.Common.Services;
using DSD.emailLog;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);


Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();


builder.Logging.ClearProviders();
builder.Logging.AddSerilog();




// Serilog
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog();


// Services
builder.Services.AddHostedService<Worker>();
builder.Services.AddTransient<EmailService>();
builder.Services.AddTransient<SqlService>();


var host = builder.Build();

// Configuration access works
string rootFilePath = builder.Configuration.GetConnectionString("rootFilepath");
//string logFilePath = Path.Combine(rootFilePath, "Emaillog-.txt");

Log.Information("✅ Host started " + rootFilePath);

host.Run();