# Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'aws-sdk-s3'
require 'spec_helper'

S3 = Snowplow::EmrEtlRunner::S3

describe S3 do
  subject { Object.new.extend described_class }

  s3 = Aws::S3::Client.new(stub_responses: {
    list_objects: { contents: [{ key: 'example1.jpg' }, { key: 'example2.jpg' }]}
  })

  describe '#empty?' do
    it 'should take a client and location argument' do
      expect(subject).to respond_to(:empty?).with(2).argument
    end

    it 'should check a folder on S3 is empty' do
      expect(subject.empty?(s3, 's3://bucket/prefix')).to eq(false)
    end
  end

  describe '#list_objects' do
    it 'should take a client and location argument' do
      expect(subject).to respond_to(:list_objects).with(2).argument
    end

    it 'should list the objects in a s3 folder' do
      expect(subject.list_objects(s3, 's3://bucket/prefix')).to eq(['example1.jpg', 'example2.jpg'])
    end
  end

  describe '#download_files' do
    it 'should take a client, a location and a local location argument' do
      expect(subject).to respond_to(:download_files).with(3).argument
    end

    it 'should download files locallly' do
      expect(subject.download_files(s3, 's3://bucket/prefix', '/tmp')
        .map { |o| File.absolute_path(o.body)}).to eq(['/tmp/example1.jpg', '/tmp/example2.jpg'])
    end
  end

  describe '#parse_bucket_prefix' do
    it 'should take a s3 url argument' do
      expect(subject).to respond_to(:parse_bucket_prefix).with(1).argument
    end

    it 'should parse the bucket and prefix' do
      expect(subject.parse_bucket_prefix('s3://bucket/prefix')).to eq(['bucket', 'prefix'])
      expect(subject.parse_bucket_prefix('s3://bucket/prefix/file.jpg')).to eq(
        ['bucket', 'prefix/file.jpg'])
    end
  end
end
