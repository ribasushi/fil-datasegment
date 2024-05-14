fil-datasegment
=======================

> A basic utility for downloading and on-the-fly assembly of [FRC58 aggregates](https://github.com/filecoin-project/FIPs/blob/master/FRCs/frc-0058.md).

## Installation

```
go install github.com/ribasushi/fil-datasegment@latest
```

## Usage Example
```
~$ curl -sL --compressed https://api.spade.storage/public/sample_bafkzcibcaapnwjc76mz43iamuegqxdcvvrdtaocebdghk25fuzdx4i2u5mgkodq_frc58.json | fil-datasegment from-manifest
2024-05-14T10:30:23.706+0200    INFO    fil-datasegment(54453)  ufcli/ufcli.go:324      === BEGIN 'from-manifest' run
2024-05-14T10:30:25.098+0200    INFO    fil-datasegment(54453)  fil-datasegment@v0.0.0-20240514082815-52a2278fdea9/dlass.go:269 about to get 14.97GiB in 19492 data segments for FRC58 aggregate bafkzcibcaapnwjc76mz43iamuegqxdcvvrdtaocebdghk25fuzdx4i2u5mgkodq
Segments total:19492 existing:... downloaded:... / ...GiB
...
```

## Use cases

This type of aggregate is currently used predominantly by `web3.storage`. If you are planning to adopt this for your project - do so with caution. Aggregation that allows non-interactive examination by 3rd parties has a number of undesirable side-effects.

## License
[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
