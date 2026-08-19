package handler

import (
	"context"
	"log/slog"

	"github.com/fujin-io/fujin/internal/proto"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
)

func HandleStream(ctx context.Context, str session.Stream, opts session.StreamOptions) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	out := proto.NewOutbound(str, opts.WriteDeadline, logger)
	h := proto.NewHandler(ctx,
		opts.PingInterval, opts.PingTimeout, opts.PingStream,
		opts.BuildVersion, opts.BaseGeneration, opts.GenerationProvider, out, str, logger,
	)
	in := proto.NewInbound(str, opts.ForceTerminateTimeout, h, logger,
		opts.AbortRead, opts.CloseRead,
	)
	go in.ReadLoop(ctx)
	out.WriteLoop()
}
