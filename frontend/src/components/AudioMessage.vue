<script setup>
import { ref, watch, onUnmounted } from 'vue'

const props = defineProps({
  audioData: {
    type: [ArrayBuffer, Uint8Array, String, Blob],
    required: true
  }
})

const audioRef = ref(null)
const audioUrl = ref('')
const isPlaying = ref(false)
const progress = ref(0)
const currentTime = ref('0:00')
const duration = ref('0:00')

const formatTime = (seconds) => {
  if (!seconds || isNaN(seconds)) return '0:00'
  const mins = Math.floor(seconds / 60)
  const secs = Math.floor(seconds % 60)
  return `${mins}:${secs < 10 ? '0' : ''}${secs}`
}

const createBlobUrl = () => {
  if (audioUrl.value) URL.revokeObjectURL(audioUrl.value)

  let blob
  if (props.audioData instanceof Blob) {
    blob = props.audioData
  } else if (typeof props.audioData === 'string') {
    const binary = atob(props.audioData)
    const bytes = new Uint8Array(binary.length)
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i)
    }
    blob = new Blob([bytes], { type: 'audio/ogg; codecs=opus' })
  } else {
    blob = new Blob([props.audioData], { type: 'audio/ogg; codecs=opus' })
  }

  audioUrl.value = URL.createObjectURL(blob)
}

watch(() => props.audioData, createBlobUrl, { immediate: true })

const togglePlay = () => {
  if (!audioRef.value) return
  if (isPlaying.value) {
    audioRef.value.pause()
  } else {
    audioRef.value.play()
  }
}

const onLoadedMetadata = () => {
  if (audioRef.value) {
    duration.value = formatTime(audioRef.value.duration)
  }
}

const onTimeUpdate = () => {
  if (audioRef.value && audioRef.value.duration) {
    progress.value = (audioRef.value.currentTime / audioRef.value.duration) * 100
    currentTime.value = formatTime(audioRef.value.currentTime)
  }
}

const onEnded = () => {
  isPlaying.value = false
  progress.value = 0
  currentTime.value = '0:00'
}

const seek = (event) => {
  if (!audioRef.value || !audioRef.value.duration) return
  const rect = event.currentTarget.getBoundingClientRect()
  const clickX = event.clientX - rect.left
  const width = rect.width
  const percentage = clickX / width
  audioRef.value.currentTime = percentage * audioRef.value.duration
}

onUnmounted(() => {
  if (audioUrl.value) URL.revokeObjectURL(audioUrl.value)
})
</script>

<template>
  <div class="tg-audio-player">
    <audio
      ref="audioRef"
      :src="audioUrl"
      @play="isPlaying = true"
      @pause="isPlaying = false"
      @loadedmetadata="onLoadedMetadata"
      @timeupdate="onTimeUpdate"
      @ended="onEnded"
    ></audio>

    <button @click="togglePlay" type="button" class="play-btn">
      <svg v-if="!isPlaying" class="icon" viewBox="0 0 24 24">
        <path fill="currentColor" d="M8 5v14l11-7z"/>
      </svg>
      <svg v-else class="icon" viewBox="0 0 24 24">
        <path fill="currentColor" d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/>
      </svg>
    </button>

    <div class="audio-info">
      <div class="progress-bar-container" @click="seek">
        <div class="progress-bar">
          <div class="fill" :style="{ width: progress + '%' }"></div>
        </div>
      </div>
      <div class="time-label">
        {{ isPlaying ? currentTime : duration }}
      </div>
    </div>
  </div>
</template>

<style scoped>
.tg-audio-player {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 190px;
  padding: 2px 0;
}

.play-btn {
  background: rgba(255, 255, 255, 0.2);
  border: none;
  color: #ffffff;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  transition: background-color 0.2s;
}

.play-btn:hover {
  background: rgba(255, 255, 255, 0.3);
}

.icon {
  width: 20px;
  height: 20px;
}

.audio-info {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.progress-bar-container {
  padding: 4px 0;
  cursor: pointer;
}

.progress-bar {
  height: 4px;
  background: rgba(255, 255, 255, 0.3);
  border-radius: 2px;
  overflow: hidden;
  position: relative;
}

.fill {
  height: 100%;
  background: #ffffff;
  border-radius: 2px;
  transition: width 0.1s linear;
}

.time-label {
  font-size: 11px;
  color: rgba(255, 255, 255, 0.8);
  font-weight: 500;
}
</style>
