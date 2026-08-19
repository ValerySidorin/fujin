package handler

import (
	"context"

	"github.com/fujin-io/fujin/internal/proto"
	"github.com/fujin-io/fujin/public/proto/fujin/v1/session"
)

func HandleStream(ctx context.Context, str session.Stream, opts session.StreamOptions) {
	out := proto.NewOutbound(str, opts.WriteDeadline, opts.Logger)
	h := proto.NewHandler(ctx,
		opts.PingInterval, opts.PingTimeout, opts.PingStream,
		opts.BaseGeneration, opts.GenerationProvider, out, str, opts.Logger,
	)
	in := proto.NewInbound(str, opts.ForceTerminateTimeout, h, opts.Logger,
		opts.AbortRead, opts.CloseRead,
	)
	go in.ReadLoop(ctx)
	out.WriteLoop()
}
