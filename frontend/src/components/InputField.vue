<script setup>
import { ref } from 'vue'

const emit = defineEmits(['send', 'send-audio'])

const text = ref('')
const isRecording = ref(false)
let mediaRecorder = null
let audioChunks = []

const handleSend = () => {
  if (!text.value.trim()) return
  emit('send', text.value.trim())
  text.value = ''
}

const toggleRecording = async () => {
  if (isRecording.value) {
    stopRecording()
  } else {
    await startRecording()
  }
}

const startRecording = async () => {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
    const mimeType = MediaRecorder.isTypeSupported('audio/ogg; codecs=opus')
      ? 'audio/ogg; codecs=opus'
      : 'audio/webm'

    mediaRecorder = new MediaRecorder(stream, { mimeType })
    audioChunks = []

    mediaRecorder.ondataavailable = (event) => {
      if (event.data.size > 0) {
        audioChunks.push(event.data)
      }
    }

    mediaRecorder.onstop = () => {
      const audioBlob = new Blob(audioChunks, { type: mimeType })
      emit('send-audio', audioBlob)

      stream.getTracks().forEach(track => track.stop())
    }

    mediaRecorder.start()
    isRecording.value = true
  } catch (err) {
    console.error('Ошибка доступа к микрофону:', err)
  }
}

const stopRecording = () => {
  if (mediaRecorder && isRecording.value) {
    mediaRecorder.stop()
    isRecording.value = false
  }
}
</script>

<template>
  <div class="input-container">
    <button 
      type="button" 
      @click="toggleRecording" 
      :class="['mic-btn', { recording: isRecording }]"
    >
      🎤
    </button>

    <input 
      v-model="text" 
      type="text" 
      :placeholder="isRecording ? 'Идет запись...' : 'Сообщение...'" 
      @keydown.enter="handleSend"
    />

    <button type="button" @click="handleSend" class="send-btn">
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

.mic-btn {
  background: transparent;
  border: none;
  font-size: 18px;
  cursor: pointer;
  padding: 4px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: transform 0.2s, background-color 0.2s;
}

.mic-btn.recording {
  background-color: #ef4444;
  animation: pulse 1.5s infinite;
}

@keyframes pulse {
  0% { transform: scale(1); }
  50% { transform: scale(1.15); }
  100% { transform: scale(1); }
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
