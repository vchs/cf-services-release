#!/bin/bash

cd $(dirname $(readlink -f $0))
rm -rf .bundle vendor/bundle
bundle install --deployment --path vendor/bundle
bundle exec rake ci:setup:rspec spec:unit
