#!/bin/bash
#file: run.sh

if [ $1 = "a" -o $1 = "analysis" ]
then
	echo "start VideoAnalyzer"
	storm jar out/artifacts/Video_jar/Video.jar com.persist.VideoAnalyzer analyzer_config.json
elif [ $1 = "g" -o $1 = "grab" ]
then
	echo "start VideoGrabber"
	storm jar out/artifacts/Video_jar/Video.jar com.persist.VideoGrabber grabber_config.json
elif [ $1 = "t" -o $1 = "thread" ]
then
	echo "start GrabThread"
	storm jar out/artifacts/Video_jar/Video.jar com.persist.GrabThread $2 $3
else
	echo "unknown command :" $1
	echo "start VideoAnalyzer: a"
	echo "start VideoGrabber: g"
	echo "start GrabThread: t"
fi
