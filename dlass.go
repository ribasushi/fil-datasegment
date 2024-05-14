package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/detailyang/go-fallocate"
	"github.com/edsrzf/mmap-go"
	"github.com/filecoin-project/go-data-segment/datasegment"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	filabi "github.com/filecoin-project/go-state-types/abi"
	"github.com/mattn/go-isatty"
	"github.com/ribasushi/go-toolbox-interplanetary/fil"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type Agg struct {
	FRC58CommP *WrCommP `json:"frc58_aggregate"`
	PieceList  []Piece  `json:"piece_list"`
}
type Piece struct {
	PcidV2  WrCommP `json:"pcid_v2"`
	Sources []WrURL `json:"sources"`
}

type pieceTask struct {
	startOffset int // we check for 64bits in util.go
	payloadSize int
	segmentSize int
	comm        *fil.CommP
	url         *url.URL
}

var (
	overwriteExisting bool
	showProgress      bool
	linearWalk        bool
	manifestFilename  string
	outFilename       string
	maxConcurrency    uint
	maxRetries        uint
	segmentTimeout    uint

	dlBytes       = new(int64)
	dlCount       = new(int64)
	existingCount = new(int64)
)

var downloadAndAssemble = &ufcli.Command{
	Usage: "Assemble FRC58 aggregate from a supplied manifest",
	Name:  "from-manifest",
	Flags: []ufcli.Flag{
		&ufcli.StringFlag{
			Name:        "manifest",
			Usage:       "Location of the aggregate manifest",
			Value:       "/dev/stdin",
			Destination: &manifestFilename,
			DefaultText: "STDIN",
		},
		&ufcli.StringFlag{
			Name:        "output",
			Usage:       "Output aggregate filename",
			Destination: &outFilename,
			DefaultText: " <aggregate_pcidv2>.car ",
		},
		&ufcli.BoolFlag{
			Name:        "overwrite",
			Usage:       "Overwrite existing output if any",
			Destination: &overwriteExisting,
		},
		&ufcli.BoolFlag{
			Name:        "in-order",
			Usage:       "Do not randomize the order of downloads - order ascending by size",
			Destination: &linearWalk,
		},
		&ufcli.UintFlag{
			Name:        "max-concurrency",
			Usage:       "How many downloads to execute in parallel",
			Value:       5,
			Destination: &maxConcurrency,
		},
		&ufcli.UintFlag{
			Name:        "max-retries",
			Usage:       "Maximum amount of retries",
			Value:       5,
			Destination: &maxRetries,
		},
		&ufcli.UintFlag{
			Name:        "segment-timeout-seconds",
			Usage:       "Maximum amount of seconds download of one segment should take",
			Value:       60 * 10, // 10 mins
			Destination: &segmentTimeout,
		},
		&ufcli.BoolFlag{
			Name:        "show-progress",
			Usage:       "Show a progress meter",
			Value:       isatty.IsTerminal(os.Stderr.Fd()),
			Destination: &showProgress,
		},
	},
	Action: func(cctx *ufcli.Context) error {

		// parse manifest
		//
		manifestFh, err := os.Open(cctx.String("manifest"))
		if err != nil {
			return cmn.WrErr(err)
		}
		defer manifestFh.Close()
		if isatty.IsTerminal(manifestFh.Fd()) {
			return xerrors.New("reading the manifest from a TTY makes little sense: check the value of your --manifest flag")
		}

		var aggManifest Agg
		if err := json.NewDecoder(manifestFh).Decode(&aggManifest); err != nil {
			return cmn.WrErr(err)
		}
		manifestFh.Close()

		if aggManifest.FRC58CommP == nil {
			return xerrors.New("for the time being input manifest must specify a `frc58_aggregate` CID")
		}

		// validate and prep list
		//
		toProccess := make([]pieceTask, len(aggManifest.PieceList))
		pis := make([]filabi.PieceInfo, len(aggManifest.PieceList))

		for i := range aggManifest.PieceList {
			ae := aggManifest.PieceList[i]
			if len(ae.Sources) != 1 {
				return xerrors.Errorf("currenly exactly one source is supported per piece, yet %s has %d", ae.PcidV2.PCidV2().String(), len(ae.Sources))
			}
			pis[i] = ae.PcidV2.PieceInfo()
			toProccess[i] = pieceTask{
				comm:        &ae.PcidV2.CommP,
				payloadSize: int(ae.PcidV2.PayloadSize()),
				segmentSize: int(ae.PcidV2.PieceInfo().Size.Unpadded()),
				url:         &ae.Sources[0].URL,
			}
		}

		aggObj, err := datasegment.NewAggregate(aggManifest.FRC58CommP.PieceInfo().Size, pis)
		if err != nil {
			return cmn.WrErr(err)
		}
		aggReifiedPcidV1, err := aggObj.PieceCID()
		if err != nil {
			return cmn.WrErr(err)
		}

		if aggReifiedPcidV1 != aggManifest.FRC58CommP.PCidV1() {
			return xerrors.Errorf("supplied list of %d pieces does not aggregate with the expected PCidV1 %s, got %s instead", len(aggManifest.PieceList), aggManifest.FRC58CommP.PCidV1(), aggReifiedPcidV1)
		}

		// prepare output file
		//
		if outFilename == "" {
			outFilename = aggManifest.FRC58CommP.PCidV2().String() + ".frc58"
		}

		oFlags := os.O_CREATE | os.O_RDWR
		if overwriteExisting {
			oFlags |= os.O_TRUNC
		}
		outFh, err := os.OpenFile(outFilename, oFlags, 0644)
		if err != nil {
			return cmn.WrErr(err)
		}
		defer func() {
			if err := outFh.Close(); err != nil {
				log.Warnf("closing %s failed: %s", outFilename, err)
			}
		}()

		fhStat, err := outFh.Stat()
		if err != nil {
			return cmn.WrErr(err)
		}
		var maybeExisting bool
		if fhStat.Size() == int64(aggObj.DealSize.Unpadded()) {
			maybeExisting = true
		} else if err := outFh.Truncate(0); err != nil {
			return cmn.WrErr(err)
		}

		if err := fallocate.Fallocate(outFh, 0, int64(aggObj.DealSize.Unpadded())); err != nil {
			return cmn.WrErr(err)
		}
		if err := outFh.Truncate(int64(aggObj.DealSize.Unpadded())); err != nil {
			return cmn.WrErr(err)
		}

		aggBuf, err := mmap.Map(outFh, mmap.RDWR, 0)
		if err != nil {
			return cmn.WrErr(err)
		}
		defer func() {
			if err := aggBuf.Flush(); err != nil {
				log.Warnf("flushing output buffer failed: %s", err)
			}
			if err := aggBuf.Unmap(); err != nil {
				log.Warnf("unmapping output buffer failed: %s", err)
			}
		}()

		// write out ToC
		//
		tocR, err := aggObj.IndexReader()
		if err != nil {
			return cmn.WrErr(err)
		}
		tocOffset := int(datasegment.DataSegmentIndexStartOffset(aggObj.DealSize))
		if _, err := io.ReadFull(tocR, aggBuf[tocOffset:]); err != nil {
			return xerrors.Errorf("failed writing ToC at offset %d: %w", tocOffset, err)
		}

		// go through tasklist, add offsets, add zero-regions if/where needed
		//
		var payloadTot int
		var lastSegmentEnd int
		for i, e := range aggObj.Index.Entries {
			toProccess[i].startOffset = int(filabi.PaddedPieceSize(e.Offset).Unpadded())
			payloadTot += toProccess[i].payloadSize

			if maybeExisting {
				if zeroGap := toProccess[i].startOffset - lastSegmentEnd; zeroGap > 0 {
					toProccess = append(toProccess, pieceTask{
						startOffset: lastSegmentEnd,
						segmentSize: zeroGap,
					})
				}
			}
			lastSegmentEnd = toProccess[i].startOffset + toProccess[i].segmentSize
		}
		if zeroGap := tocOffset - lastSegmentEnd; zeroGap > 0 {
			toProccess = append(toProccess, pieceTask{
				startOffset: lastSegmentEnd,
				segmentSize: zeroGap,
			})
		}

		if linearWalk {
			sort.Slice(toProccess, func(i, j int) bool {
				return toProccess[i].segmentSize < toProccess[j].segmentSize
			})
		} else {
			rand.Shuffle(len(toProccess), func(i, j int) {
				toProccess[i], toProccess[j] = toProccess[j], toProccess[i]
			})
		}

		// pull data
		//
		totTodo := len(aggObj.Index.Entries)
		log.Infof("about to get %.02fGiB in %d data segments for FRC58 aggregate %s", float64(payloadTot)/(1<<30), totTodo, aggManifest.FRC58CommP.PCidV2())

		eg, ctx := errgroup.WithContext(cctx.Context)

		// separate from the errgroup
		progressStop := make(chan struct{})
		printProgress := func() {
			fmt.Fprintf(os.Stderr, "Segments total:%d existing:%d downloaded:%d / %.02fGiB\r", totTodo, atomic.LoadInt64(existingCount), atomic.LoadInt64(dlCount), float64(atomic.LoadInt64(dlBytes))/(1<<30))
		}
		if showProgress {
			go func() {
				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-progressStop:
						return
					case <-ctx.Done():
						return
					case <-ticker.C:
						printProgress()
					}
				}
			}()
		}

		eg.SetLimit(int(maxConcurrency))
		for _, p := range toProccess {
			if ctx.Err() != nil {
				break
			}
			p := p
			eg.Go(func() error {
				ctx, closer := context.WithDeadline(ctx, time.Now().Add(time.Duration(segmentTimeout)*time.Second))
				defer closer()
				return doTask(ctx, p, aggBuf[p.startOffset:p.startOffset+p.segmentSize], maybeExisting)
			})
		}

		err = eg.Wait()

		if showProgress {
			close(progressStop)
			printProgress()
			os.Stderr.WriteString("\n")
		}

		return err
	},
}

func doTask(ctx context.Context, p pieceTask, segmentBuf []byte, maybeExisting bool) error {

	// just a zerofill
	//
	if p.comm == nil {
		zeroRegion(segmentBuf)
		return nil
	}

	cpHasher := new(commp.Calc)

	// preexisting data: see if what we have on disk is good
	//
	if maybeExisting {
		cpHasher.Write(segmentBuf)
		cp, _, err := cpHasher.Digest()
		if err != nil {
			return cmn.WrErr(err)
		}
		if bytes.Equal(cp, p.comm.Digest()) {
			atomic.AddInt64(existingCount, 1)
			return nil // what we have on disk is already good
		}

		// otherwise zero the box and proceed
		zeroRegion(segmentBuf)
	}

	// actual HTTP call
	//
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url.String(), nil)
	if err != nil {
		return cmn.WrErr(err)
	}

	client := retryingClient(int(maxRetries), 3*time.Second, 15*time.Second)

	resp, err := client.Do(request)
	if err != nil {
		return cmn.WrErr(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return xerrors.Errorf("HTTP error code %d", resp.StatusCode)
	}

	if _, err = io.ReadFull(
		io.TeeReader(resp.Body, cpHasher),
		segmentBuf[:int(resp.ContentLength)],
	); err != nil {
		return cmn.WrErr(err)
	}

	if tooSmall := 127 - resp.ContentLength; tooSmall > 0 {
		cpHasher.Write(make([]byte, tooSmall))
	}
	cp, _, err := cpHasher.Digest()
	if err != nil {
		return cmn.WrErr(err)
	}
	if !bytes.Equal(cp, p.comm.Digest()) {
		return xerrors.Errorf("download of %d bytes from %s resulted in CommP %02X, expected %02X", resp.ContentLength, p.url, cp, p.comm.Digest())
	}

	atomic.AddInt64(dlBytes, resp.ContentLength)
	atomic.AddInt64(dlCount, 1)
	return nil
}

// lazy 32MiB zero-filler
var zeroez = make([]byte, 32<<20)

func zeroRegion(t []byte) {
	var off int
	remain := len(t)
	for remain > 0 {
		segLen := len(zeroez)
		if remain < segLen {
			segLen = remain
		}
		copy(t[off:off+segLen], zeroez)
		off += segLen
		remain -= segLen
	}
}
