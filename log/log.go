package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"os"
)

var (
	levelMap = map[string]zapcore.Level{
		"debug":   zapcore.DebugLevel,
		"error":   zapcore.ErrorLevel,
		"warning": zapcore.WarnLevel,
		"info":    zapcore.InfoLevel,
		"panic":   zapcore.PanicLevel,
		"fatal":   zapcore.FatalLevel,
	}
	Log = zap.L()
)

type Config struct {
	File        string `help:"日志输出文件" default:"$ROOT/running.log"`
	FileSize    int    `help:"日志文件大小限制,单位MB" default:"500"`
	FileBackups int    `help:"最大保留日志文件数量" default:"10"`
	FileAge     int    `help:"日志文件保留天数" default:"0"`
	Level       string `help:"日志输出级别,可选[debug|info|warning|error|fatal]" releaseDefault:"info" default:"debug"`
	Development bool   `help:"日志打开开发模式" releaseDefault:"false" default:"true"`
	Output      string `help:"日志输出方式,可选[any|file|console]"  devDefault:"any" testDefault:"file" default:"file"`
	Encoder     string `help:"日志输出格式,可选[json|console]" releaseDefault:"json" default:"console"`
}

func NewLog(cfg Config) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("01-02 15:04:05")
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	var encoder zapcore.Encoder
	if cfg.Encoder == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	}
	//文件writeSyncer
	fileWriteSyncer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.File,        //日志文件存放目录
		MaxSize:    cfg.FileSize,    //文件大小限制,单位MB
		MaxBackups: cfg.FileBackups, //最大保留日志文件数量
		MaxAge:     cfg.FileAge,     //日志文件保留天数
		Compress:   false,           //是否压缩处理
	})

	level := zapcore.InfoLevel
	if _level, ok := levelMap[cfg.Level]; ok {
		level = _level
	}
	consoleOutput := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), level)
	fileOutput := zapcore.NewCore(encoder, fileWriteSyncer, level)
	var core zapcore.Core
	switch cfg.Output {
	case "any":
		core = zapcore.NewTee(consoleOutput, fileOutput)
		break
	case "file":
		core = fileOutput
		break
	default:
		core = consoleOutput
	}
	if cfg.Development {
		Log = zap.New(core, zap.Development(), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	} else {
		Log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	}
	return Log
}
