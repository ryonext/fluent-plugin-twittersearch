module Fluent
    class TwittersearchError < StandardError
    end
    class TwittersearchInput < Input
        Plugin.register_input('twittersearch', self)
        config_param :consumer_key, :string
        config_param :consumer_secret, :string
        config_param :oauth_token, :string
        config_param :oauth_token_secret, :string
        config_param :tag, :string
        config_param :keyword, :string,:default => nil
        config_param :hashtag, :string,:default => nil
        config_param :user_id, :string,:default => nil
        config_param :count,   :integer
        config_param :run_interval,   :integer
        config_param :result_type, :string
        config_param :media, :bool ,:default => false
        config_param :latest_id_file, :string,:default => nil

        attr_reader :twitter

        def initialize
            super
            require "twitter"
        end

        def configure(config)
            super
            Twitter.configure do |cnf|
                cnf.consumer_key    = @consumer_key
                cnf.consumer_secret = @consumer_secret
                cnf.oauth_token = @oauth_token
                cnf.oauth_token_secret = @oauth_token_secret
                # https://github.com/muffinista/chatterbot/issues/11
                cnf.connection_options = Twitter::Default::CONNECTION_OPTIONS.merge(
                  :request => { :open_timeout => 5, :timeout => 10}
                )
            end
            raise Fluent::ConfigError.new if @keyword.nil? and @hashtag.nil? and @user_id.nil?
            @latest_id = ((@latest_id_file && File.exists?(@latest_id_file)) ? File.open(@latest_id_file).read : '0').to_i
        end

        def start
            super
            @thread = Thread.new(&method(:run))
        end

        def search(twitter_client)
            search_option = {:count => @count, :result_type => @result_type}
            tweets = []
            begin
              results = if @user_id.nil?
                res = twitter_client.search(@keyword.nil? ? "##{@hashtag}" : @keyword, search_option)
                res.results
              else
                twitter_client.user_timeline(@user_id, search_option)
              end
            rescue
              $log.warn "raises exception: #{$!.class}, '#{$!.message}'"
              return tweets
            end
            results.reverse_each do |result|

                tweet = Hash.new
                [:id,:retweet_count,:favorite_count].each do |key|
                    tweet.store(key.to_s, result[key].to_s)
                end
                [:screen_name,:profile_image_url,:profile_image_url_https].each do |key|
                    tweet.store(key.to_s, result.user[key].to_s)
                end
                tweet.store('created_at', result[:created_at].strftime("%Y-%m-%d %H:%M:%S"))
                tweet.store('user_id', result.user[:id])
                tweet.store('text',result.text.force_encoding('utf-8'))
                tweet.store('name',result.user.name.force_encoding('utf-8'))
                tweet.store('tweet_url', "https://twitter.com/#{tweet['screen_name']}/status/#{tweet['id']}")
                tweet.store('media_url', '')
                original_uri = result.user.attrs[:status][:entities][:urls].first[:expanded_url]
                tweet.store('original_url', original_uri )

                if @media && !result.media.empty?
                  result.media.each do |m|
                    begin
                      media_tweet = tweet.dup
                      media_tweet.store('media_url', m.media_url)
                      tweets << media_tweet
                    rescue
                      $log.warn "raises exception: #{$!.class}, '#{$!.message}'"
                      tweets << tweet
                      break
                    end
                  end
                else
                  tweets << tweet
                end
            end
            tweets
        end

        def run
            loop do
                search(Twitter::Client.new).each do |tweet|
                    if @latest_id_file.nil?
                      #emit tweet
                      Engine.emit @tag,Engine.now,tweet
                      next
                    end

                    current_id = tweet['id'].to_i
                    if @latest_id < current_id
                      #emit newest tweet
                      Engine.emit @tag,Engine.now,tweet
                      @latest_id = current_id
                      write_latest_id_file
                    end
                end
                sleep @run_interval
            end
        end

        def shutdown
            Thread.kill(@thread)
            write_latest_id_file
        end

        def write_latest_id_file
            File.open(@latest_id_file, 'w') {|file| file.print(@latest_id)} if @latest_id_file
        end
    end
end
