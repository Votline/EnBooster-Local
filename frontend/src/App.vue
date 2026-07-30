<script setup>
import { ref, nextTick, onMounted } from 'vue'
import Background from './components/Background.vue'
import MainBox from './components/MainBox.vue'
import InputField from './components/InputField.vue'
import MessageItem from './components/MessageItem.vue'

const chatContainer = ref(null)
const messages = ref([])
let socket = null

const scrollToBottom = async () => {
    await nextTick()
    if (chatContainer.value) {
        chatContainer.value.scrollTop = chatContainer.value.scrollHeight
    }
}

onMounted(() => {
    socket = new WebSocket('ws://localhost:8080/ws')

    socket.onopen = () => {
        console.log('🟢 WS соединен с Go!')
    }

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data)

            const botMsg = data.req_trace 
                ? messages.value.find(m => m.req_trace === data.req_trace && !m.isMe)
                : null

            if (botMsg) {
                botMsg.text = data.text
                if (data.ogg_bytes) botMsg.audioData = data.ogg_bytes
            } else {
                messages.value.push({
                    req_trace: data.req_trace,
                    text: data.text,
                    audioData: data.ogg_bytes || null,
                    isMe: false
                })
            }
            scrollToBottom()
        } catch (err) {
            console.error('Ошибка парсинга JSON:', err)
        }
    }

    socket.onerror = (err) => {
        console.error('🔴 Ошибка WS:', err)
    }
})

const onSend = (text) => {
    if (!text.trim()) return

    messages.value.push({
        text: text,
        isMe: true
    })
    scrollToBottom()

    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ text: text }))
    } else {
        console.error('Сокет не активен!')
    }
}

const onSendAudio = async (blob) => {
    messages.value.push({
        audioData: blob,
        isMe: true
    })
    scrollToBottom()

    const reader = new FileReader()
    reader.readAsDataURL(blob)
    reader.onloadend = () => {
        const base64Audio = reader.result.split(',')[1]

        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({
                ogg_bytes: base64Audio
            }))
        } else {
            console.error('Сокет не активен!')
        }
    }
}
</script>

<template>
    <Background>
        <MainBox>
            <div ref="chatContainer" class="messages-container">
                <MessageItem 
                    v-for="(msg, index) in messages" 
                    :key="index" 
                    :text="msg.text" 
                    :audio-data="msg.audioData"
                    :is-me="msg.isMe" 
                />
            </div>
            <InputField @send="onSend" @send-audio="onSendAudio" />
        </MainBox>
    </Background>
</template>

<style>
html, body, #app {
    width: 100%;
    height: 100%;
    margin: 0;
    padding: 0;
    overflow: hidden;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

.messages-container {
    flex: 1;
    overflow-y: auto;
    padding: 16px 28px;
    display: flex;
    flex-direction: column;
}

.messages-container::-webkit-scrollbar {
    width: 6px;
}

.messages-container::-webkit-scrollbar-track {
    background: transparent;
}

.messages-container::-webkit-scrollbar-thumb {
    background: #2b2b30;
    border-radius: 4px;
}

.messages-container::-webkit-scrollbar-thumb:hover {
    background: #3a3a40;
}
</style>
