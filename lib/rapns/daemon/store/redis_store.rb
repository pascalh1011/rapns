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
      MultiJson.dump(self.attributes.merge(type: self.class.to_s, id: SecureRandom.random_number(2**32)))
    end

    module ClassMethods
      def load_from_redis(redis_value)
        attributes = MultiJson.load(redis_value)
        attributes['data'] = MultiJson.load(attributes['data']) if attributes['data']
        instance = attributes['type'].constantize.new(attributes)
        instance.created_at = Time.parse(attributes['created_at'])
        instance.retries = attributes['retries'].to_i
        instance.id = attributes['id'].to_i

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
          delayed_notifications = []

          expired_threshold = Rapns.config.stalled_notification_tolerence.seconds.ago

          with_redis_reconnect_and_retry do |redis|
            while (notifications.length < Rapns.config.batch_size && newest_redis_item = redis.rpop(Rapns::REDIS_LIST_NAME))
              notification = Rapns::Apns::Notification.load_from_redis(newest_redis_item)

              if notification.deliver_after && notification.deliver_after > Time.now
                delayed_notifications << newest_redis_item
                next
              end

              unless notification.created_at < expired_threshold || (notification.expiry && notification.created_at + notification.expiry.seconds < Time.now)
                notifications << notification
              end
            end

            # Add notifications back into the list that we're not ready to process yet
            redis.rpush(Rapns::REDIS_LIST_NAME, delayed_notifications) unless delayed_notifications.empty?
          end

          notifications
        end

        def retry_after(notification, deliver_after)
          notification.deliver_after = deliver_after
          notification.retries += 1

          with_redis_reconnect_and_retry do |redis|
            redis.rpush(REDIS_LIST_NAME, notification.dump_to_redis)
          end
        end

        def mark_delivered(notification)
          Rapns.logger.info "Mark delivered: #{notification.inspect}"
        end

        def mark_failed(notification, code, description)
          Rapns.logger.info "Mark failed permanently: #{notification.inspect}"
        end

        def create_apns_feedback(failed_at, device_token, app)
          with_database_reconnect_and_retry do
            Rapns::Apns::Feedback.create!(:failed_at => failed_at, :device_token => device_token)
          end
        end

        def create_gcm_notification(attrs, data, registration_ids, deliver_after, app)
          notification = Rapns::Gcm::Notification.new
          notification.assign_attributes(attrs)
          notification.data = data
          notification.registration_ids = registration_ids
          notification.deliver_after = deliver_after
          notification.app = app

          with_redis_reconnect_and_retry do |redis|
            redis.rpush(REDIS_LIST_NAME, notification.dump_to_redis)
          end
          
          notification
        end

        def after_daemonize
          
        end
        protected
      end
    end
  end
end