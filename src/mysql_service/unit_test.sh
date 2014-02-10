#!/bin/bash
set -x
set -e

cd $(dirname $(readlink -f $0))
rm -rf .bundle vendor/bundle
bundle install --deployment --path vendor/bundle
bundle exec rake ci:setup:rspec spec:unit
tar czvf reports.tgz spec/reports
