import Fastify from 'fastify';
import WebSocket from 'ws';
import fs from 'fs';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import twilio from 'twilio';

// Load environment variables from .env file
dotenv.config();

const {
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    PHONE_NUMBER_FROM,
    PHONE_NUMBER_TEST1,
    DOMAIN: rawDomain,
    OPENAI_API_KEY,
    PROMPT,
} = process.env;

// Constants
const DOMAIN = rawDomain.replace(/(^\w+:|^)\/\//, '').replace(/\/+$/, ''); // Clean protocols and slashes
const SYSTEM_MESSAGE = PROMPT;
const VOICE = 'ballad'
const PORT = process.env.PORT || 5050; // Allow dynamic port assignment
const outboundTwiML = `<?xml version="1.0" encoding="UTF-8"?><Response><Connect><Stream url="wss://${DOMAIN}/media-stream" /></Connect></Response>`;

// List of Event Types to log to the console. See the OpenAI Realtime API Documentation.
const LOG_EVENT_TYPES = [
    'error',
    'response.content.done',
    'rate_limits.updated',
    'response.done',
    'input_audio_buffer.committed',
    'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started',
    'session.created'
];

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !PHONE_NUMBER_FROM || !rawDomain || !OPENAI_API_KEY) {
    console.error('One or more environment variables are missing. Please ensure TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, PHONE_NUMBER_FROM, DOMAIN, and OPENAI_API_KEY are set.');
    process.exit(1);
}

// Function to check if a number is allowed to be called. With your own function, be sure
// to do your own diligence to be compliant.
async function isNumberAllowed(to) {
    try {

        // Uncomment these lines to test numbers. Only add numbers you have permission to call
        const consentMap = {"+18005551212": true}
        if (consentMap[to]) return true;

        // Check if the number is a Twilio phone number in the account, for example, when making a call to the Twilio Dev Phone
        const incomingNumbers = await client.incomingPhoneNumbers.list({ phoneNumber: to });
        if (incomingNumbers.length > 0) {
            return true;
        }

        // Check if the number is a verified outgoing caller ID. https://www.twilio.com/docs/voice/api/outgoing-caller-ids
        const outgoingCallerIds = await client.outgoingCallerIds.list({ phoneNumber: to });
        if (outgoingCallerIds.length > 0) {
            return true;
        }

        return false;
        } catch (error) {
        console.error('Error checking phone number:', error);
        return false;
    }
}

// Function to make an outbound call
async function makeCall(to) {
    try {
    //   const isAllowed = await isNumberAllowed(to);
    //   if (!isAllowed) {
    //     console.warn(`The number ${to} is not recognized as a valid outgoing number or caller ID.`);
    //     process.exit(1);
    //   }

      const call = await client.calls.create({
        from: PHONE_NUMBER_TEST1,
        to,
        twiml: outboundTwiML,
      });
      console.log(`Call started with SID: ${call.sid}`);
    } catch (error) {
      console.error('Error making call:', error);
    }
}

// Initialize the Twilio library and set our outgoing call TwiML
const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

// Initialize Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Root Route
fastify.get('/', async (request, reply) => {
    reply.send({message: 'Twilio Media Stream Server is running!'});
});

// WebSocket route for media-stream
fastify.register(async (fastify) => {
    fastify.get('/media-stream', {websocket: true}, (connection, req) => {
        console.log('Client connected');
        const openAiWs = new WebSocket('wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01', {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`,
                "OpenAI-Beta": "realtime=v1"
            }
        });

        let streamSid = null;

        const sendSessionUpdate = () => {
            const sessionUpdate = {
                type: 'session.update',
                session: {
                    turn_detection: { type: 'server_vad' },
                    input_audio_format: 'g711_ulaw',
                    output_audio_format: 'g711_ulaw',
                    voice: VOICE,
                    instructions: SYSTEM_MESSAGE,
                    modalities: ["text", "audio"],
                    temperature: 0.8,
                }
            };

            console.log('Sending session update:', JSON.stringify(sessionUpdate));
            openAiWs.send(JSON.stringify(sessionUpdate));

            const initialConversationItem = {
                type: 'conversation.item.create',
                item: {
                    type: 'message',
                    role: 'user',
                    content: [
                        {
                            type: 'input_text',
                            text: 'Greet the user with "Hello there!" and ask if you are speaking to the correct person'
                            // text: 'Greet the user and tell them you have something important to say. Then pass on the message'
                        }
                    ]
                }
            };

            openAiWs.send(JSON.stringify(initialConversationItem));
            openAiWs.send(JSON.stringify({ type: 'response.create' }));
        };
        // Open event for OpenAI WebSocket
        openAiWs.on('open', () => {
            console.log('Connected to OpenAI Realtime API');
            setTimeout(sendSessionUpdate, 100); // Ensure connection stability, send after .25 seconds
        });
        // Listen for messages from the OpenAI WebSocket (and send to Twilio if necessary)
        openAiWs.on('message', (data) => {
            try {
                const response = JSON.parse(data);
                if (LOG_EVENT_TYPES.includes(response.type)) {
                    console.log(`Received event: ${response.type}`, response);
                }
                if (response.type === 'session.updated') {
                    console.log('Session updated successfully:', response);
                }
                if (response.type === 'response.audio.delta' && response.delta) {
                    const audioDelta = {
                        event: 'media',
                        streamSid: streamSid,
                        media: {payload: Buffer.from(response.delta, 'base64').toString('base64')}
                    };
                    connection.send(JSON.stringify(audioDelta));
                }
            } catch (error) {
                console.error('Error processing OpenAI message:', error, 'Raw message:', data);
            }
        });
        // Handle incoming messages from Twilio
        connection.on('message', (message) => {
            try {
                const data = JSON.parse(message);
                switch (data.event) {
                    case 'media':
                        if (openAiWs.readyState === WebSocket.OPEN) {
                            const audioAppend = {
                                type: 'input_audio_buffer.append',
                                audio: data.media.payload
                            };
                            openAiWs.send(JSON.stringify(audioAppend));
                        }
                        break;
                    case 'start':
                        streamSid = data.start.streamSid;
                        console.log('Incoming stream has started', streamSid);
                        break;
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
            }
        });
        // Handle connection close
        connection.on('close', () => {
            if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
            console.log('Client disconnected.');
        });
        // Handle WebSocket close and errors
        openAiWs.on('close', () => {
            console.log('Disconnected from the OpenAI Realtime API');
        });
        openAiWs.on('error', (error) => {
            console.error('Error in the OpenAI WebSocket:', error);
        });
    });
});

// Initialize server
fastify.listen({ port: PORT }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${PORT}`);

    // Parse command-line arguments to get the phone number
    const args = process.argv.slice(2);
    const phoneNumberArg = args.find(arg => arg.startsWith('--call='));
    if (!phoneNumberArg) {
        console.error('Please provide a phone number to call, e.g., --call=+18885551212');
        process.exit(1);
    }
    console.log(
        'Our recommendation is to always disclose the use of AI for outbound or inbound calls.\n'+
        'Reminder: all of the rules of TCPA apply even if a call is made by AI \n' +
        'Check with your counsel for legal and compliance advice.'
    );
    const phoneNumberToCall = phoneNumberArg.split('=')[1].trim();
    console.log('Calling ', phoneNumberToCall);
    makeCall(phoneNumberToCall);
});