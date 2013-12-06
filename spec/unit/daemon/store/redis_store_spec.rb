require 'unit_spec_helper'

require 'rapns/daemon/store/redis_store'

describe Rapns::Daemon::Store::RedisStore, mock_redis: true do
  let(:store) { Rapns::Daemon::Store::RedisStore.new }

  before do
    Rapns::Notification.send(:include, Rapns::NotificationAsRedisObject)
    @iphone_app = Rapns::Apns::App.create!(name: 'iphone_app', environment: 'development', certificate: TEST_CERT)
    @android_app = Rapns::Gcm::App.create!(name: 'android_app', environment: 'development', auth_key: 'RANDOMAUTHKEY')
    Redis.current.del(Rapns::REDIS_LIST_NAME)

  end

  def create_gcm_notification(attrs = {})
    notification = Rapns::Gcm::Notification.new({app: @android_app, registration_ids: ['abcdefg'], data: { message: 'Hello there' }, collapse_key: '12345'}.merge(attrs))
    notification.created_at = attrs[:created_at] if attrs[:created_at]

    notification.save_to_redis
    notification
  end

  def create_ios_notification(attrs = {})
    notification = Rapns::Apns::Notification.new({app: @iphone_app, device_token: 'a'*64, alert: 'New Message', badge: 2, expiry: 1.day.to_i, data: { blah: 10 }}.merge(attrs))
    notification.created_at = attrs[:created_at] if attrs[:created_at]
    notification.save_to_redis
    notification
  end

  it 'adds an iOS push notification to the queue' do
    notification = create_ios_notification

    stored_notification = Rapns::Apns::Notification.load_from_redis(Redis.current.rpop(Rapns::REDIS_LIST_NAME))
    
    stored_notification.should be_kind_of(Rapns::Apns::Notification)
    stored_notification.device_token.should == 'a'*64
    stored_notification.alert.should == 'New Message'
    stored_notification.badge.should == 2
    stored_notification.expiry.should == 1.day.to_i
    stored_notification.attributes_for_device.should == { 'blah' => 10 }
  end

  it 'adds an Android push notification to the queue' do
    notification = create_gcm_notification

    stored_notification = Rapns::Apns::Notification.load_from_redis(Redis.current.rpop(Rapns::REDIS_LIST_NAME))
    stored_notification.should be_kind_of(Rapns::Gcm::Notification)
    stored_notification.registration_ids.should == ['abcdefg']
    stored_notification.data.should == { 'message' => 'Hello there' }
    stored_notification.collapse_key.should == '12345'
  end

  context 'daemon' do
    it 'returns a batch of deliverable items from the queue' do
      Rapns.config.batch_size = 3
      5.times { create_ios_notification }

      items = store.deliverable_notifications([])

      items.collect(&:device_token).should include 'a'*64
      items.length.should == 3
    end

    it 'permanently forgets about items that are expired or too old' do
      create_ios_notification(created_at: 1.day.ago)
      create_ios_notification(expiry: 1, created_at: 10.seconds.ago)

      Redis.current.llen(Rapns::REDIS_LIST_NAME).should == 2
      store.deliverable_notifications([]).length.should == 0
    end
  end
end
