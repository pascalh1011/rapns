require 'redis'

require 'rapns/daemon/store/redis_store/reconnectable'
require 'rapns/daemon/store/active_record/reconnectable'

# require 'active_support/core_ext/marshal'

module Rapns
  REDIS_LIST_NAME = 'rapns:notifications'

  module NotificationAsRedisObject
    def self.included(base)
      base.extend ClassMethods
    end

    def save_to_redis
      self.created_at ||= Time.now

      Rapns.with_redis { |redis| redis.rpush(REDIS_LIST_NAME, dump_to_redis) } if valid?
    end

    def dump_to_redis
      MultiJson.dump(self.attributes)
    end

    module ClassMethods
      def load_from_redis(redis_value)
        attributes = MultiJson.load(redis_value)
        attributes['data'] = MultiJson.load(attributes['data']) if attributes['data']
        instance = attributes['type'].constantize.new(attributes)
        instance.created_at = Time.parse(attributes['created_at'])

        instance
      rescue MultiJson::LoadError, LoadError
        Rails.logger.error "[RAPNS]: Attempted to parse invalid Redis object #{redis_value.inspect}"
        nil
      end
    end

  end

  module Daemon
    module Store
      class RedisStore
        include Rapns::Daemon::Store::RedisStore::Reconnectable
        include Rapns::Daemon::Store::ActiveRecord::Reconnectable

        def deliverable_notifications(apps)
          notifications = []
          expired_threshold = Rapns.config.stalled_notification_tolerence.seconds.ago

          with_redis_reconnect_and_retry do |redis|
            while (notifications.length < Rapns.config.batch_size && newest_redis_item = Redis.current.rpop(Rapns::REDIS_LIST_NAME))
              notification = Rapns::Apns::Notification.load_from_redis(newest_redis_item)

              unless notification.created_at < expired_threshold || (notification.expiry && notification.created_at + notification.expiry.seconds < Time.now)
                notifications << notification
              end
            end
          end

          notifications

          # redis_values = with_redis_reconnect_and_retry do | redis|

          #   batch_size = [redis.llen(PENDING_QUEUE_NAME), Rapns.config.batch_size].min
          #   redis_values = redis.lrange(PENDING_QUEUE_NAME, 0, batch_size-1)

          #   unless redis_values.empty?
          #     time_score = Time.now.utc.to_i
          #     redis_values_with_scores = redis_values.collect { |value| [time_score, value] }
          #     redis.zadd PROCESSING_QUEUE_NAME, redis_values_with_scores.flatten
          #     redis.ltrim PENDING_QUEUE_NAME, batch_size, redis.llen(PENDING_QUEUE_NAME)
          #   end

          #   move_retries_into_pending(redis)
          #   handle_stalled_notifications(redis)

          #   redis_values

          # end

          # build_notifications redis_values
        end

        def retry_after(notification, deliver_after)
          with_redis_reconnect_and_retry do |redis|
            redis.zrem PROCESSING_QUEUE_NAME, notification.dump_redis_value

            notification.retries += 1
            notification.deliver_after = deliver_after

            redis.zadd RETRIES_QUEUE_NAME, deliver_after.utc.to_i, notification.dump_redis_value
          end
        end

        def mark_delivered(notification)
          remove_notification_in_processing(notification)
        end

        def mark_failed(notification, code, description)
          remove_notification_in_processing(notification)
          # with_database_reconnect_and_retry do
          #   #return if Rapns::Notification.exists?(notification.id)
          #   notification.delivered = false
          #   notification.delivered_at = nil
          #   notification.failed = true
          #   notification.failed_at = Time.now
          #   notification.error_code = code
          #   notification.error_description = description
          #   notification.save(:validate => false)
          # end
        end

        def create_apns_feedback(failed_at, device_token, app)
          #with_database_reconnect_and_retry do
          #  Rapns::Apns::Feedback.create!(:failed_at => failed_at, :device_token => device_token, :app => app)
          #end
        end

        def create_gcm_notification(attrs, data, registration_ids, deliver_after, app)
          notification = Rapns::Gcm::Notification.new
          notification.assign_attributes(attrs)
          notification.data = data
          notification.registration_ids = registration_ids
          notification.deliver_after = deliver_after
          notification.app = app
          with_redis_reconnect_and_retry do |redis|
            redis.rpush PENDING_QUEUE_NAME, notification.dump_redis_value
          end
          notification
        end

        def after_daemonize
          #reconnect_database
        end

        def build_notifications(list_of_notif_hashes)
          list_of_notif_hashes.collect do |notif_hash|
            Rapns::Notification.marshal_redis_value(notif_hash)
          end
        end

        protected

        def remove_notification_in_processing(notification)
          with_redis_reconnect_and_retry do |redis|
            puts notification.dump_redis_value.inspect
            redis.zrem PROCESSING_QUEUE_NAME, notification.dump_redis_value
          end
        end

        def move_retries_into_pending(redis)
          retries = redis.zrangebyscore(RETRIES_QUEUE_NAME, '-inf', Time.now.utc.to_i)
          unless retries.empty?
            redis.lpush(PENDING_QUEUE_NAME, retries)
            redis.zrem(RETRIES_QUEUE_NAME, retries)
          end
        end

        def handle_stalled_notifications(redis)
          feedback_poll_interval = Rapns.config.feedback_poll
          stalled_notification_tolerence = Rapns.config.stalled_notification_tolerence

          min_score = stalled_notification_tolerence.seconds.ago.utc.to_i
          max_score = feedback_poll_interval.seconds.ago.utc.to_i

          tolerated_stalled_notifications = redis.zrangebyscore(PROCESSING_QUEUE_NAME, min_score, max_score)
          unless tolerated_stalled_notifications.empty?
            redis.lpush(PENDING_QUEUE_NAME, tolerated_stalled_notifications)
            redis.zrem(PROCESSING_QUEUE_NAME, tolerated_stalled_notifications)
          end

          untolerated_stalled_notifications = redis.zrangebyscore(PROCESSING_QUEUE_NAME, '-inf', min_score)
          redis.zrem(PROCESSING_QUEUE_NAME, untolerated_stalled_notifications) unless untolerated_stalled_notifications.empty?
        end

      end
    end
  end
end