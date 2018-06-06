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
require 'contracts'
require 'pathname'
require 'uri'

module Snowplow
  module EmrEtlRunner
    module S3

      include Contracts

      # Check a location on S3 is empty.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder to check for emptiness
      Contract Aws::S3::Client, String => Bool
      def empty?(client, location)
        bucketName, prefix = parse_bucket_prefix(location)
        bucket = Aws::S3::Bucket.new({name: bucketName, client: client})
        not bucket.objects({prefix: prefix}).limit(1).any?
      end

      # List the files in a location on S3.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder where listing will happen
      Contract Aws::S3::Client, String => ArrayOf[String]
      def list_objects(client, location)
        bucketName, prefix = parse_bucket_prefix(location)
        client.list_objects({bucket: bucketName, prefix: prefix}).contents.map { |c| c.key }
      end

      # Download the files in a location on S3 to a local directory.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder to download
      # +localDir+:: local directory to download the files to
      Contract Aws::S3::Client, String, String => ArrayOf[Aws::S3::Types::GetObjectOutput]
      def download_files(client, location, localDir)
        bucketName, prefix = parse_bucket_prefix(location)
        bucket = Aws::S3::Bucket.new({name: bucketName, client: client})
        path = Pathname.new(localDir)
        bucket.objects({prefix: prefix}).map do |object|
          filename = Pathname.new(object.key).basename
          object.get({response_target: (path + filename).to_s})
        end
      end

      # Extract the bucket and prefix from an S3 url.
      #
      # Parameters:
      # +location+:: the S3 url to parse
      Contract String => [String, String]
      def parse_bucket_prefix(location)
        u = URI.parse(location)
        return u.host, u.path[1..-1]
      end

    end
  end
end
