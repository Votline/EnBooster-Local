<script setup>
import { ref } from 'vue'

const emit = defineEmits(['send'])
const text = ref('')

const handleSend = () => {
    if (!text.value.trim()) return
    emit('send', text.value.trim())
    text.value = ''
}
</script>

<template>
    <div class="input-container">
        <input 
            v-model="text" 
            type="text" 
            placeholder="Сообщение..." 
            @keydown.enter="handleSend"
        />
        <button @click="handleSend" class="send-btn">
            ↑
        </button>
    </div>
</template>

<style scoped>
.input-container {
    display: flex;
    align-items: center;
    gap: 8px;
    background-color: #1e1f20;
    padding: 14px 18px;
    border-radius: 24px;
    margin: 25px;
}

input {
    flex: 1;
    background: transparent;
    border: none;
    outline: none;
    color: #ffffff;
    font-size: 15px;
    padding: 4px 8px;
}

input::placeholder {
    color: #8e8e93;
}

.send-btn {
    background-color: #3390ec;
    color: #ffffff;
    border: none;
    border-radius: 50%;
    width: 32px;
    height: 32px;
    display: flex;
    justify-content: center;
    align-items: center;
    cursor: pointer;
    font-weight: bold;
    font-size: 16px;
    transition: background-color 0.2s;
}

.send-btn:hover {
    background-color: #2b7bc5;
}
</style>
