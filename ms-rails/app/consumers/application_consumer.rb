# frozen_string_literal: true

# Application consumer from which all Karafka consumers should inherit
# You can rename it if it would conflict with your current code base (in case you're integrating
# Karafka with other frameworks)
class ApplicationConsumer < Karafka::BaseConsumer

  private def consume
    messages.each do |message|
      puts message.payload
      product = Product.new(message.payload)
      if Product.exists?(id: product.id)
        Product.find(product.id).update(product.attributes)
      else
        Product.create(product.attributes)
      end
    end
  end
  
end
