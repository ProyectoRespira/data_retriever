import tweepy
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

AQI_LEVELS = [
    (0, 50, "🟢 Bueno 😁"),  # Good
    (51, 100, "🟡 Moderado 🙂"),  # Moderate
    (101, 150, "🟠 Insalubre para grupos sensibles 😷"),  # Unhealthy for sensitive groups
    (151, 200, "🔴 Insalubre 😶‍🌫️"),  # Unhealthy
    (201, 300, "🟣 Muy insalubre 😰"),  # Very Unhealthy
    (301, 500, "⚫ Peligroso 💀")  # Hazardous
]

def get_aqi_label(aqi_value):
    for min_val, max_val, label in AQI_LEVELS:
        if min_val <= aqi_value <= max_val:
            return label
    return "Fuera del AQI 💀🤯"  

def get_latest_aqi_summary(df):
    avg_value = int(df['forecast_avg'].mean())
    max_value = int(df['forecast_avg'].max())
    min_value = int(df['forecast_avg'].min())
    
    return avg_value, max_value, min_value

def construct_message(avg_aqi, max_aqi, min_aqi):
    avg_label = get_aqi_label(avg_aqi)
    max_label = get_aqi_label(max_aqi)
    min_label = get_aqi_label(min_aqi)

    message = (
        f"📊 Calidad del Aire para Gran Asunción - Próximas 12 hs\n"
        f"🔹 AQI Promedio: {avg_aqi} ({avg_label})\n"
        f"🔺 AQI Máximo: {max_aqi} ({max_label})\n"
        f"🔻 AQI Mínimo: {min_aqi} ({min_label})\n\n"
        "🔗 ¡Pronto podrás acceder al pronóstico en tu zona! www.proyectorespira.net"
    )
    return message

def send_message(msg=''):
    consumer_key = get_secret_value('TWITTER_CONSUMER_KEY')
    consumer_secret = get_secret_value('TWITTER_CONSUMER_SECRET')
    access_token = get_secret_value('TWITTER_ACCESS_TOKEN')
    access_token_secret = get_secret_value('TWITTER_ACCESS_TOKEN_SECRET')
    
    client = tweepy.Client(consumer_key=consumer_key,
                            consumer_secret=consumer_secret,
                            access_token=access_token,
                            access_token_secret=access_token_secret)
    response = client.create_tweet(text=msg)
    return response

@custom
def transform_custom(data, *args, **kwargs):
    klogger = kwargs.get('logger')
    try:
        aqi_summary = get_latest_aqi_summary(data)
        if aqi_summary:
            avg_aqi, max_aqi, min_aqi = aqi_summary
        message = construct_message(avg_aqi, max_aqi, min_aqi)
        response = send_message(msg=message)
        klogger.info(f'Twitter response: {response}')
    except Exception as e:
        klogger.exception(f'An error occurred: {e}')

