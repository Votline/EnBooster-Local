<template>
    <div class="main-box">
        <slot />
    </div>
</template>

<style scoped>
.main-box {
    width: 95%;
    height: 100%;
    background-color: #0f0f0f;
    border-radius: 16px;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4);
}
</style>
