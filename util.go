package main

import (
	"math/bits"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
)

func init() {
	if bits.UintSize != 64 {
		panic("program relies on Int being 64bit (for mmapped buffer access)")
	}
}

type WrCommP struct {
	fil.CommP
}

func (out *WrCommP) UnmarshalJSON(b []byte) error {
	c, err := cid.Decode(string(b[1 : len(b)-1]))
	if err != nil {
		return cmn.WrErr(err)
	}
	p, err := fil.CommPFromPCidV2(c)
	if err != nil {
		return cmn.WrErr(err)
	}
	*out = WrCommP{p}
	return nil
}

type WrURL struct {
	url.URL
}

func (out *WrURL) UnmarshalJSON(b []byte) error {
	u, err := url.Parse(string(b[1 : len(b)-1]))
	if err != nil {
		return cmn.WrErr(err)
	}
	*out = WrURL{*u}
	return nil
}

func retryingClient(attempts int, waitMin, waitMax time.Duration) *http.Client {
	rc := retryablehttp.NewClient()
	rc.Logger = &retLogWrap{ipfslog: log}
	rc.RetryWaitMin = waitMin
	rc.RetryWaitMax = waitMax
	rc.RetryMax = attempts
	rc.CheckRetry = retryablehttp.ErrorPropagatedRetryPolicy
	return rc.StandardClient()
}

type retLogWrap struct{ ipfslog *logging.ZapEventLogger }

func (w *retLogWrap) Error(msg string, kv ...interface{}) { w.ipfslog.Errorw(msg, kv...) }
func (w *retLogWrap) Info(msg string, kv ...interface{})  { w.ipfslog.Infow(msg, kv...) }
func (w *retLogWrap) Debug(msg string, kv ...interface{}) { w.ipfslog.Debugw(msg, kv...) }
func (w *retLogWrap) Warn(msg string, kv ...interface{})  { w.ipfslog.Warnw(msg, kv...) }
