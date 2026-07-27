<script setup>
import { ref, nextTick } from 'vue'
import Background from './components/Background.vue'
import MainBox from './components/MainBox.vue'
import InputField from './components/InputField.vue'
import MessageItem from './components/MessageItem.vue'

const chatContainer = ref(null)

const messages = ref([
    { id: 1, text: 'Здорово! Чем помочь?', isMe: false },
    { id: 2, text: 'Привет, да вот верстаю чат на Vue', isMe: true },
    { id: 3, text: 'Красава, вырисовывается четко.', isMe: false }
])

const scrollToBottom = async () => {
    await nextTick()
    if (chatContainer.value) {
        chatContainer.value.scrollTop = chatContainer.value.scrollHeight
    }
}

const onSend = (text) => {
    messages.value.push({
        id: Date.now(),
        text: text,
        isMe: true
    })
    scrollToBottom()
}
</script>

<template>
    <Background>
        <MainBox>
            <div ref="chatContainer" class="messages-container">
                <MessageItem 
                    v-for="msg in messages" 
                    :key="msg.id" 
                    :text="msg.text" 
                    :is-me="msg.isMe" 
                />
            </div>
            <InputField @send="onSend" />
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
