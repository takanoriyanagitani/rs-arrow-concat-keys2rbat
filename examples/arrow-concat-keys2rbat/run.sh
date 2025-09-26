#!/bin/bash

genkeys(){
	bkt=$1
	date=$2

	dname="./sample.d/bkt=${bkt}/date=${date}"
	mkdir -p "${dname}"
	filename="${dname}/keys.int64.be.dat"

	exec 11>&1
	exec 1>"${filename}"

	printf '0123456789abcdef' | xxd -r -ps
	printf '1123456789abcdef' | xxd -r -ps
	printf '2123456789abcdef' | xxd -r -ps
	printf '3123456789abcdef' | xxd -r -ps

	exec 1>&11
	exec 11>&-
}

geninput(){
	echo generating input files...

	mkdir -p ./sample.d

	bkt0=cafef00d-0000-beaf-face-864299792458
	bkt1=cafef00d-0001-beaf-face-864299792458
	bkt2=cafef00d-0002-beaf-face-864299792458

	genkeys "${bkt0}" "2025-09-24"
	genkeys "${bkt0}" "2025-09-25"
	genkeys "${bkt0}" "2025-09-26"

	genkeys "${bkt1}" "2025-09-24"
	genkeys "${bkt1}" "2025-09-25"
	genkeys "${bkt1}" "2025-09-26"

	genkeys "${bkt2}" "2025-09-24"
	genkeys "${bkt2}" "2025-09-25"
	genkeys "${bkt2}" "2025-09-26"

}

test -f "./sample.d/bkt=cafef00d-0002-beaf-face-864299792458/date=2025-09-26" ||
	geninput

./arrow-concat-keys2rbat \
	--base-dir-name ./sample.d \
	--bucket-prefix 'bkt=' \
	--date-prefix 'date=' \
	--date '2025-09-24' \
	--bucket-column-name 'bucket' \
	--key-column-name 'identifier' \
	--basename 'keys.int64.be.dat'
